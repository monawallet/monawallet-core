//
//  BRPeerManager.c
//
//  Created by Aaron Voisine on 9/2/15.
//  Copyright (c) 2015 breadwallet LLC.
//
//  Permission is hereby granted, free of charge, to any person obtaining a copy
//  of this software and associated documentation files (the "Software"), to deal
//  in the Software without restriction, including without limitation the rights
//  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//  copies of the Software, and to permit persons to whom the Software is
//  furnished to do so, subject to the following conditions:
//
//  The above copyright notice and this permission notice shall be included in
//  all copies or substantial portions of the Software.
//
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
//  THE SOFTWARE.

#include "BRPeerManager.h"
#include "BRBloomFilter.h"
#include "BRSet.h"
#include "BRArray.h"
#include "BRInt.h"
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include <limits.h>
#include <time.h>
#include <assert.h>
#include <pthread.h>
#include <errno.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>

#define PROTOCOL_TIMEOUT      20.0
#define MAX_CONNECT_FAILURES  20 // notify user of network problems after this many connect failures in a row
#define CHECKPOINT_COUNT      (sizeof(checkpoint_array)/sizeof(*checkpoint_array))
#define DNS_SEEDS_COUNT       (sizeof(dns_seeds)/sizeof(*dns_seeds))
#define GENESIS_BLOCK_HASH    (UInt256Reverse(u256_hex_decode(checkpoint_array[0].hash)))
#define PEER_FLAG_SYNCED      0x01
#define PEER_FLAG_NEEDSUPDATE 0x02

#if BITCOIN_TESTNET

static const struct { uint32_t height; const char *hash; uint32_t timestamp; uint32_t target; } checkpoint_array[] = {
    { 0, "a2b106ceba3be0c6d097b2a6a6aacf9d638ba8258ae478158f449c321061e0b2", 1488924140, 0x1e0ffff0 },
    { 10, "63dd272d5b1e63ab72416d4b20fde1e08ae8ccbf3fecba2af1f36b1e70a375d4", 1499433884, 0x1e0ffff0 },
    { 20, "040417c7685f90448f3d58ce60780a8766ab14d995f7063c525ad5e194de65d4", 1499437832, 0x1e0ffff0 },
    { 30, "83b80264c9903c6282beca0a49928fd9fae1e58531f4a82476e6d4c2e0e7c5b6", 1499441223, 0x1e0ffff0 }
};

static const char *dns_seeds[] = {
    "monacoin.org.", "testnet-dnsseed.monacoin.org."
};

#else // main net

// blockchain checkpoints - these are also used as starting points for partial chain downloads, so they need to be at
// difficulty transition boundaries in order to verify the block difficulty at the immediately following transition
static const struct { uint32_t height; const char *hash; uint32_t timestamp; uint32_t target; } checkpoint_array[] = {
    { 0, "ff9f1c0116d19de7c9963845e129f9ed1bfc0b376eb54fd7afa42e0d418c8bb6", 1388479472, 0x1e0ffff0 },
    { 20160, "84deef4beace3e6e2ab45d0ccbe09a358f6e43b94cf2301d835ef0e4cd54f16d", 1389880987, 0x1c74a8fd },
    { 40320, "7c4b3a6a87c21fd0a58213c18d9b5edadc38468e59bb79c15472e7a22ca9869d", 1391642914, 0x1c3abd3f },
    { 60480, "b228924c8fc2ecabe259d6f8751c81fd4f467327c3d75683a38a9b54910c263b", 1393401682, 0x1c1cb4d8 },
    { 80640, "cf20c57e8c262573d8d787e0d5bb02346610d8870e035d431de429b4e4b25571", 1395187018, 0x1c11656b },
    { 100800, "3e3cfc1e6573fe128865c9273bb1126120096afe706da98798be2decc56c276f", 1397003984, 0x1c0da7a6 },
    { 120960, "53b0f973631883646cba45082dd9df1f7be9a0afa52a9caee625b8940634255e", 1398815958, 0x1c0d7298 },
    { 141120, "3d0caa2c607337a610d3bec72a5e4ce6e4fab7bb5948acf873f64559bb9e5c26", 1400638469, 0x1c0f2b61 },
    { 161280, "c0023d8afae4e3714066e7a40731ec7571893ea641662daa98db989e13ff4b02", 1402543914, 0x1c07eb6e },
    { 181440, "f67f0f768b4fe1d2eb3e6c69d78476908c2f27e3c51af56e4adb37dc215f2b7b", 1404456186, 0x1c0736c5 },
    { 201600, "061b8b99b37e5234a4d32775d74c3a45703e34b89e796eafbcd6ca0804426e7c", 1406373989, 0x1c0125c5 },
    { 221760, "fc5117f359f582f29f013c0041fe2c7e5654969d187a91ae2a8036e29573c75a", 1408482602, 0x1b4587d8 },
    { 241920, "ee306f11e42ab6add9dedd7509d46521eb97504a2122f7bb253a3f7d5b7a6304", 1410888885, 0x1b72c206 },
    { 262080, "8090cf6d15c18941108f001e4b7526f3db92df4c0e86b2de2e74e20567c045c3", 1413610113, 0x1c0092a0 },
    { 282240, "0f3244adc8518e9c53cd7dc0db72301bcdfbe47b0ab42d751e8b1a70e2101446", 1416121884, 0x1b677e65 },
    { 302400, "ffff3d65d170d8a515d77a0439dee9f6a969097f82b6061f8e1a2cd9a9ef6024", 1418879501, 0x1c008127 },
    { 322560, "27164ef84d2b4825999f05f536a63f6d74b226960ce5d5dda885453209ebe90a", 1422618051, 0x1b43a0f0 },
    { 342720, "f676d960e85a5e84bf3693ba7c48fbb0247e84e7efee4572212c96ce39ec3d84", 1425576438, 0x1b11c353 },
    { 362880, "a7d66d2906491e5452929efb9cbe330032073f7535d43c58f4e469d6aa53eb48", 1427940543, 0x1b24ef3b },
    { 383040, "fc80552105d6555a1c7be6b0ad42b142942879c75d4e917609801cba95ba1588", 1430358024, 0x1b1eafc7 },
    { 403200, "67c31a52f881d020dcaf3ab14e027f4d0710a16263502611d8688d8b86ae9e2a", 1433578614, 0x1b26ac5d },
    { 423360, "0f400afbe27e4a14f8e597fade2ae093bc72aa544185658f57ea404d7fdea415", 1436640771, 0x1b2b5177 },
    { 443520, "7f97b54c3d57aeab040d9eb8a2892a74fb944a5bf762fdefc4cd0835007b8101", 1441219824, 0x1b1958c3 },
    { 463680, "9771e55dddd76a05b58fdde6e24da36fdb5a2b5566cae5393485dc32137323d4", 1444630068, 0x1b7fc52e },
    { 483840, "33a5398463f2ca179159f4a7de022609f55c8e78bc625c38a30067d615bb1dfc", 1446537758, 0x1b5cdaa2 },
    { 504000, "e3d813e519a79b8bc4d7b35bb923c9992dc4c0bcca8d07d23ae9c10660d52408", 1448443781, 0x1b5e2d4d },
    { 524160, "91fa8b419207bd36d87281aa0835b73163689c5fd891c55ba0131a02be39db1c", 1450346487, 0x1b4160f7 },
    { 544320, "f11827b1990742e0399bd1f94733cd4045d45ce4bae9bddb685464ae2158de7c", 1452247924, 0x1b3e2602 },
    { 564480, "8d0af24450a1cd65f8ac8b7f96af3e77976131fc0386ef17c5fa1d99ca9ed967", 1454150556, 0x1b550122 },
    { 584640, "92a8e90683177d79a7a7f2191718ca52811308e06c9f432d939d411d5730d25f", 1456052596, 0x1c010d76 },
    { 604800, "02d0510ca18d081b67df44a3f4798c55597a25ce9bba442de825068e0cbd81ad", 1457956652, 0x1c00f8f3 },
    { 624960, "e0ef3922bb6fde76839b9d56e842480924cf93ac67d62bb5921a19a441c13c95", 1459857960, 0x1c00c06d },
    { 645120, "8021c0480e382177e3d82b68a6746ea56237dbb7951f34e70df9efedf0076e3e", 1461758514, 0x1b7e0765 },
    { 665280, "96fd2cf8e0f1aa3b13ef8571002cdcd7fc2a85a3a6de443fe789afa5a20a0e96", 1463662301, 0x1c010f6d },
    { 685440, "ef0dabbc9393552f1aec09e48e4c10db1efe1e3e0a247727b6977017a11eb1d9", 1465562743, 0x1c00923b },
    { 705600, "937ea6d2256dfbe8f3495fe545cce9964be566967a33055ee66bf8b8e1129d46", 1467463781, 0x1b6c29a1 },
    { 725760, "630cafc520b7ac9dc222e6a38da11c05502eccb90719d45e55bac121875be812", 1469363832, 0x1b4ac71e },
    { 745920, "8f42e053078f6ac5fb11f825d4fa7b8da3e0e7cbb4960b5c8179fa2ad57fc81e", 1471265155, 0x1b4c25a3 },
    { 766080, "7df05173bbea25690d10efea3b6314d7e04c890a217ba357ede92b5c3f35ac61", 1473169791, 0x1b7c69f2 },
    { 786240, "d80b37f544932a5f207323f2cdd32ad694a7461ee233c0f2f32d463a62b1d2a6", 1475074549, 0x1c0096b2 },
    { 806400, "4987de18a5ee0896c252a09521fd568312046b142c5b36ed73b6000685ca3479", 1476975211, 0x1b622a9c },
    { 826560, "18b8d334b73cbd40dd04a3c6831e2c70a45cef889401216c1eb0fc927b690df1", 1478883912, 0x1b6dc290 },
    { 846720, "921feaee9336ad75a2838c06b865c3bf3e9d26a966aa6e8d72562cdb4c127259", 1480783534, 0x1b5cbde7 },
    { 866880, "2906ab0dce8fc63213588a756bad7d0e9b1453b79a9cd6a43f0f9a61a10be202", 1482683595, 0x1b654b38 },
    { 887040, "517755cb4c0f5893bab3b65a865e5929464678e6223db4761f0db31dbd7b678f", 1484584189, 0x1b70922d },
    { 907200, "cae1cc406fd228fe57e44b157c5f204772002b0c67f99cd5fcc18452fe748bac", 1486526968, 0x1c00864f },
    { 927360, "1559a1e61da8060e21aec86bc071630858eecf96f4910218a697f07fb45d66d4", 1488463085, 0x1b4a6531 },
    { 947520, "41ae9c7b3ced403a21bdfcfe6fc07c29eba9a2abc489c5cc347c006722f87c39", 1490412058, 0x1c008d2c },
    { 967680, "6d8606a9ad195a8ef3820279be0748dd726ffc5397db22b0857ef915c4a9e511", 1492331353, 0x1b2a2d5a },
    { 987840, "2bbdaec34b9a10daaaff7e0f94b600b9ea9780841c0a9551bb63babea1dda58c", 1494234866, 0x1b52712a },
    { 1008000, "0941421e644b7a1a872b2809a1ae43e8da3dbf4d925fae34caa9cacd2859c614", 1496138162, 0x1b18edc8 },
    { 1028160, "bbff6a403af011a910db0ee795dc9501879e9ce4b305f507fb040ddd8bc3d19d", 1498040477, 0x1b0b0a62 },
    { 1048320, "b29480fcdc26752d203e3464cda458be13aa564cadc04c61bcd8fe2c7331856a", 1499944289, 0x1b071821 },
    { 1068480, "3b3d28595629b11106ae4eb994c92f273eab370f1cf947654b5017b9d4b2cc3c", 1501849339, 0x1b0fb54f },
    { 1088640, "e2a7b185a55b72f9a46e93c896d6c602d83ac910639c8b9d19baa40fa2b0cdce", 1503753490, 0x1b0b0b06 },
    { 1108800, "5be09a3965912fbe9c56d41140df592e92bc6b72b57fd96dda163623ac83a9e7", 1505659451, 0x1b07dbad },
    { 1128960, "9821a6ffb32313c7a452ec0460954a317f5a89740a5e0537d95e3bdb8250169e", 1507573237, 0x1b11a604 },
    { 1149120, "ab338f9dbab94687479ef01768e5975c05e68664d86f45a25da679429d8eb04f", 1509481030, 0x1b02be15 },
    { 1169280, "9b84ef2142d308b3b1c6fba34af0cdb013ac2121d2ece90938ffd447a152ed8d", 1511391511, 0x1b023d79 },
    { 1189440, "a7820c63aa26edaea1ca93538b2d8dc2ce82421665c5a8b90ace6e90b0455bdc", 1513302251, 0x1b01b164 },
    { 1209600, "3946bcc31371f8fc5227ae29cd74b0e80acb73973ab70130041c0212d55d6a8c", 1515211040, 0x1b01cb85 },
    { 1229760, "63b1ae280c2ff1c7ae0c8a5d75d3b4a998a038f23e52f6689675800153396d33", 1517121533, 0x1b01a4d5 },
    { 295680, "408a5c078ec57f6ced82c54a351059eab5802bc0a62b1076fe3c51cf7e3487c5", 1417964444, 0x1c00c176 },
    { 306240, "f42fe549b1b0a83822effa56e9c653dd16e2597503802972bf9e98360bf2b0d9", 1419479640, 0x1b71fabc },
    { 316800, "438cb77ff8b2e7562ba7ff926f862323ae82d15fe5cfe07d5051f0e8a119bd49", 1421434787, 0x1b5358bb },
    { 327360, "e85d1acd4ef32cd4435b5bde3e1df1276a63a21c3d5ce1f10709dc03124a027f", 1423391987, 0x1b3206ba },
    { 337920, "2228965d9475fa42c660f0d78a142d639fd8cdfcb5ccbd07e293656e20bcfc95", 1424952563, 0x1b2f6560 },
    { 348480, "38aa7887e327e4a7580cdc0e93c7f16469353e4e7cf3ff177c3d3029155e5d0d", 1426356911, 0x1b1db8e0 },
    { 359040, "da3f203cfcb4a8e6f4ba5bb1a686a1153e960cb93c827b2509cfe66d28130440", 1427523886, 0x1b1b17be },
    { 369600, "4cf47c8ff1feb739f4b44ecdeb6a7830f30c9aa9f760d12ded181809ceb6f45d", 1428801302, 0x1b1de6aa },
    { 380160, "c7474bce9d1ac38055268b935b97d666af56a831a20f78f6a92995502c606691", 1430008148, 0x1b1e189b },
    { 390720, "8fb06dc0c1fa5445c877e0fdccb117ea41666c1476129647231251be9cb1b86e", 1431317672, 0x1b2879cd },
    { 401280, "50bd1aa4589d0aab35d8706f3c07408d351fd0266e20a7cfbf8e8054fcdccf57", 1433178348, 0x1b1f2a7b },
    { 411840, "2c15afe7cfa9db83f15c6f522366c8bc3cd3d331e9473659d29c2183cbda111f", 1434873589, 0x1b2dbaa0 },
    { 422400, "806df00932bebe2798af904b9ba6c031cab17666f8a1a24e2a762f94dff86d75", 1436469792, 0x1b506450 },
    { 432960, "7cd6eb9fe3f082babf2c49400c3a0913d538091071665e886650c57b21eb3ded", 1438617358, 0x1b2a0317 },
    { 443520, "7f97b54c3d57aeab040d9eb8a2892a74fb944a5bf762fdefc4cd0835007b8101", 1441219824, 0x1b1958c3 },
    { 454080, "d2bf8e7e124210888643a23a9bab2ed9f7c0bd128ec64aed697f461705f14f35", 1443720227, 0x1b60794c },
    { 464640, "b081e046e5865419e2007822f5f9b30a4094aaacd1fddec8e93fd1207d0ffd72", 1444720678, 0x1b6bd409 },
    { 475200, "4ca33b7407b8bc4be66c13af1f77ad37e3a16f1bae632dbd0459259689ad0824", 1445720178, 0x1b6c7d0e },
    { 485760, "40d83da58bfa79e2c492c62cf211555d815ff6ad95d139485eff632af87d5b2b", 1446719224, 0x1b560a98 },
    { 496320, "44c4d458b9ce8379633d82dfd1ebbf2074549c2cd59ad4044ea09ef6ecf64983", 1447717679, 0x1b44f256 },
    { 506880, "af1332d5cbbea4b0525195d895ec9b1ecd16d841a9ea1301d913f6cca365e2ba", 1448715285, 0x1b4bd385 },
    { 517440, "7763379ea639cceaab870569ae2d644a2f503e50a9a1d6a026bc540e6b7c79b7", 1449712619, 0x1b54b669 },
    { 528000, "7402045a17dcd85d4736a83656378e74627be5f1782323a09bf999d22eebbca1", 1450709020, 0x1b45e094 },
    { 538560, "c34acdd31246b26e3981893eeb5f87f93c5414666920c58f7c58347b7395e418", 1451705069, 0x1b4a79ea },
    { 549120, "fa30336d62624d83631011150ce0678dc69069172b3c9fa1dcce910ff077e1ef", 1452700975, 0x1b3b1c6a },
    { 559680, "0455569d5a28f59371911aee8ba6d49e485ae86ae72088aafa7a527b48ade00f", 1453697501, 0x1b52dd72 },
    { 570240, "0304db427a53e9c43b69d365a1693daae0458ce79919b66b3de3fd3f436c9002", 1454693617, 0x1b4fa559 },
    { 580800, "4f25628c43408d161cef41811a460d9ccd30d46e2a03329ff119a9f4e90d77a0", 1455690137, 0x1c009888 },
    { 591360, "0569d62f6d0a3dabbaedfdfe702d6cae2f048d3e74746fb43fac008d494230ee", 1456686319, 0x1c009f46 },
    { 601920, "f51c476804db8e5ecd8bf6238f8e51eafc693fd5623055c3482b2058411c6d96", 1457684156, 0x1c00cacf },
    { 612480, "9a180ce47e360d89e82350dd61ae3a066d7d226b28ec4246903661645c847eff", 1458681585, 0x1c00ffc3 },
    { 623040, "194bfc5817791d4510fb3b5fcba3646ba6595a727071c781dba5e6e1f7d06d54", 1459676741, 0x1c00db7d },
    { 633600, "49d0d786f5df0c00763c66feffa3f481d8199919a85b68e76d0dc3663bcd5d58", 1460672146, 0x1c009812 },
    { 644160, "ed57811f20dbf20fa640b77118234e5eed83aee731f9e24c1e243cd052cd1095", 1461668109, 0x1c00b177 },
    { 654720, "8bc178e64bf90e1efce784f66257f001c9c499481f183614a740839376344a7c", 1462664686, 0x1c00913d },
    { 665280, "96fd2cf8e0f1aa3b13ef8571002cdcd7fc2a85a3a6de443fe789afa5a20a0e96", 1463662301, 0x1c010f6d },
    { 675840, "67987e5383be96c3259377920478a9a674cf0654f02f135a8bd2cfc12b70e104", 1464657996, 0x1c00c490 },
    { 686400, "e155606e8aa6f12307fe71a810bbc3c3fe7c0e1ff7385271655069eef06385d3", 1465653354, 0x1c008a88 },
    { 696960, "77c53ccce8ecb3dd04b52bdcbe7ed6c2c17865cee16cf7a494ea760c3330e6a9", 1466649674, 0x1c008dbd },
    { 707520, "34346196dc6e64dbfc64bba3033111aad4eeec8485648f19866015f48bad5179", 1467645064, 0x1b5b0797 },
    { 718080, "7fe501673ea94a767f9d1582ab7e1d28f2bf3edaa413ed2a4647f498e62c048f", 1468639909, 0x1b7367a4 },
    { 728640, "809adb0a529af79b31780282e837dfe3d6fe0344fa8822057a0923750687d742", 1469635846, 0x1c00c135 },
    { 739200, "35563aad8c083c1859eac99b88b2961412ed7620c19f89d67b58e493660e3b90", 1470632099, 0x1c009d97 },
    { 749760, "8e55fe69213da2fca5225117b33f3e13207f4a7617bd8528dd7546b5693cc82f", 1471627528, 0x1c00924a },
    { 760320, "11be5ea08fc8bebbbfc3f72f7a650f3c8eaf931dc51914ca1db8449bb0425695", 1472626237, 0x1b6a0348 },
    { 770880, "a66a618e1d88d6b4452cb83c614fba35dd0f68209ba0d4d5922e21aaa761b88f", 1473623723, 0x1b64fbdb },
    { 781440, "54c14889f0c71d884866fbd3774f60866383b4c9fb823ce8ef2ff632dc0a8064", 1474621548, 0x1b7d7ab6 },
    { 792000, "d5104d1f3ebc4baebb619490b6fc1d357409ea06d4362fad1f1bf1c913a02ff4", 1475618262, 0x1c009a12 },
    { 802560, "496ec556e86394d143f9652e38cb41e9e4919c1145556b419b637ede48d612f6", 1476613114, 0x1b666d2f },
    { 813120, "7606cb1ac70a4e6807b361af37d2f99e20aa6a0720f5785f652329d8469ff1fd", 1477612330, 0x1b476a13 },
    { 823680, "ffc717c4713a765391a3096b07915ee94af1657f66a3d4f97954a3edb2c15e4e", 1478612667, 0x1b7661ba },
    { 834240, "2cede9473e53608f8aaadf20a74fdcfe1f1dc3502820e3c65a7f0fa03768e224", 1479607692, 0x1b5e3b1d },
    { 844800, "5bfc9aa1fb5c8844e3da08b894fbe74f843c374681cbb3c42b91fa1e6b432e25", 1480602591, 0x1b43623c },
    { 855360, "d44dd07a24f322abb8c2ca9990f0b36714da27033490f8f5900a14f24aa05376", 1481598893, 0x1b751fc4 },
    { 865920, "be161679af050c4dc89355f5b233a33c6cc171e7fa1e45ac3d5c70c492581108", 1482593050, 0x1b73705a },
    { 876480, "ec2dbcdb7a3f6c2a4a298b45a833ccfe31363898d8b398d9755f6dcbd63d10e8", 1483589245, 0x1b76d199 },
    { 887040, "517755cb4c0f5893bab3b65a865e5929464678e6223db4761f0db31dbd7b678f", 1484584189, 0x1b70922d },
    { 897600, "6445f780e0270d757619258d47467a476f691f371578bc80bcbb01a01e1143b8", 1485604580, 0x1b67bece },
    { 908160, "a939d977a2f33c1135d2d3aec57491b6997051cb12316d3dbc8d26a7e6e499f3", 1486622857, 0x1c00da2c },
    { 918720, "36421951cc41624297de4b017d7a41751a1b541acc4ef8de827c149d0a05a37c", 1487629467, 0x1b67ca90 },
    { 929280, "d7ea042e2710d3b5f3f09f4b48a154a9efaf5cbd492447ce3a6cb89ae9959d07", 1488645506, 0x1b403fcc },
    { 939840, "1be16502bfcf64590c42be4fa1c7fbf3707ae1a07f87d55c7447fd216d698fa0", 1489668154, 0x1b618027 },
    { 950400, "52cbabb6d214414c9d065092ee291179ab5c6c8d601c0dddf9c34cb78a5f3f5b", 1490688224, 0x1b79557d },
    { 960960, "140e017d39519273a284baad6394feb772407a6f7cede8fbba7fe007343060d6", 1491697453, 0x1b621e41 },
    { 971520, "6cbb2c65d4603f3b15cd7de286a9edf47847f91c68360264b51cf64010b22e85", 1492692996, 0x1b1d1505 },
    { 982080, "ba6213004a2dbfa8cf49bd70e39129ac4e801f4e59bd1fcb365d817bd80ba28e", 1493689335, 0x1b437045 },
    { 992640, "16303b72a059cd980d8d3b7430d41f4294b6516841f0163c2f18650b5f53b6e9", 1494686721, 0x1b16a959 },
    { 1003200, "dbf6f42164e50d7fe5ca1a69598dc4d401db2e2fb950e6329a89df013090c86f", 1495684056, 0x1b24edd9 },
    { 1013760, "b61b4f1ffa0ba8c6a863e2beaaac2e9dbfb41e9aaa5b66c6ea96aa5e04ca2c9b", 1496681788, 0x1b1e46a3 },
    { 1024320, "f7f7859aaca01c9cbad99eeb11ebc8d444097761dce28d30033823889b19a59f", 1497678562, 0x1b19469e },
    { 1034880, "f51c4899bf4384002aaeb1456bc9b2ba62293d777a70f982f174b1d77a32de82", 1498675250, 0x1b0ee7c5 },
    { 1045440, "328b6af7dd3e316803fdf4104cd719791c7f080503e80d1d67bc5ccbed3991c4", 1499671741, 0x1b0668a7 },
    { 1056000, "3114cd0aa091a5fff9bd1025a8a2b1c096ea58f3186051b7aac3c814d23c3fd6", 1500670450, 0x1b0d0932 },
    { 1066560, "5d3fad90a7fc25c7be9083b0c57b2763375edc6ed2a331785c937b7ae17a6aef", 1501667957, 0x1b10addd },
    { 1077120, "3d972591f8c97ea009565ac793630cd2887f6965801133b3da177ed37764213d", 1502666829, 0x1b136f4b },
    { 1087680, "ef3b172fea0eba89f19082cd683e4e96d477c2a20dbb68de26aea8b610a608b3", 1503662999, 0x1b118328 },
    { 1098240, "e236185a1c546ad0d4cdeaf4026d72cd087e1e692c8548c865b54b228d8b3764", 1504659298, 0x1b1199b0 },
    { 1108800, "5be09a3965912fbe9c56d41140df592e92bc6b72b57fd96dda163623ac83a9e7", 1505659451, 0x1b07dbad },
    { 1119360, "a2a15079645d13a25caa57ca25b9a7590266b15c85b72edc791e0be0f3000366", 1506664602, 0x1b0f7339 },
    { 1129920, "b271c89f120994ca8c20d008689ad4a1e8f7be6789234a56d4db82910eda7cdd", 1507663188, 0x1b0744a7 },
    { 1140480, "5b7ea73e2f670fe15f3943b40ef97ab0a0046f5da45c99f839b63f99fcc08619", 1508661301, 0x1b02ea33 },
    { 1151040, "b4cb3bcf6f22703618249e26feae2aad0143f12586989ff77200885277d7f7d2", 1509662171, 0x1b028af5 },
    { 1161600, "2ce1d7f58d7d36046b4d2cf7b0cfedcb6b2dcd5154ed494694bf8600ea7f9117", 1510663876, 0x1b023019 },
    { 1172160, "503ac76f081a00f6468ec370769f02b371b42c9c4a10259e9e957125a0bf4efb", 1511664200, 0x1b01e95b },
    { 1182720, "8cf3c3d6afc29ecd40a4192bb5d049011ba256eb193bd4cfc1393b49fd7bd242", 1512666098, 0x1b01477a },
    { 1193280, "f168f149d261b9ea8e9781af17cedd176b39d299a5d9d1c870b545814ada1fdf", 1513665426, 0x1b00d444 },
    { 1203840, "362826819033b08ea247af34adfaa2a3f35791f21105a1eb8e22a1e86e4df13d", 1514664570, 0x1b013999 }
};

static const char *dns_seeds[] = {
    "monacoin.org.", "dnsseed.monacoin.org."
};

#endif

typedef struct {
    BRPeerManager *manager;
    const char *hostname;
    uint64_t services;
} BRFindPeersInfo;

typedef struct {
    BRPeer *peer;
    BRPeerManager *manager;
    UInt256 hash;
} BRPeerCallbackInfo;

typedef struct {
    BRTransaction *tx;
    void *info;
    void (*callback)(void *info, int error);
} BRPublishedTx;

typedef struct {
    UInt256 txHash;
    BRPeer *peers;
} BRTxPeerList;

// true if peer is contained in the list of peers associated with txHash
static int _BRTxPeerListHasPeer(const BRTxPeerList *list, UInt256 txHash, const BRPeer *peer)
{
    for (size_t i = array_count(list); i > 0; i--) {
        if (! UInt256Eq(list[i - 1].txHash, txHash)) continue;

        for (size_t j = array_count(list[i - 1].peers); j > 0; j--) {
            if (BRPeerEq(&list[i - 1].peers[j - 1], peer)) return 1;
        }
        
        break;
    }
    
    return 0;
}

// number of peers associated with txHash
static size_t _BRTxPeerListCount(const BRTxPeerList *list, UInt256 txHash)
{
    for (size_t i = array_count(list); i > 0; i--) {
        if (UInt256Eq(list[i - 1].txHash, txHash)) return array_count(list[i - 1].peers);
    }
    
    return 0;
}

// adds peer to the list of peers associated with txHash and returns the new total number of peers
static size_t _BRTxPeerListAddPeer(BRTxPeerList **list, UInt256 txHash, const BRPeer *peer)
{
    for (size_t i = array_count(*list); i > 0; i--) {
        if (! UInt256Eq((*list)[i - 1].txHash, txHash)) continue;
        
        for (size_t j = array_count((*list)[i - 1].peers); j > 0; j--) {
            if (BRPeerEq(&(*list)[i - 1].peers[j - 1], peer)) return array_count((*list)[i - 1].peers);
        }
        
        array_add((*list)[i - 1].peers, *peer);
        return array_count((*list)[i - 1].peers);
    }

    array_add(*list, ((BRTxPeerList) { txHash, NULL }));
    array_new((*list)[array_count(*list) - 1].peers, PEER_MAX_CONNECTIONS);
    array_add((*list)[array_count(*list) - 1].peers, *peer);
    return 1;
}

// removes peer from the list of peers associated with txHash, returns true if peer was found
static int _BRTxPeerListRemovePeer(BRTxPeerList *list, UInt256 txHash, const BRPeer *peer)
{
    for (size_t i = array_count(list); i > 0; i--) {
        if (! UInt256Eq(list[i - 1].txHash, txHash)) continue;
        
        for (size_t j = array_count(list[i - 1].peers); j > 0; j--) {
            if (! BRPeerEq(&list[i - 1].peers[j - 1], peer)) continue;
            array_rm(list[i - 1].peers, j - 1);
            return 1;
        }
        
        break;
    }
    
    return 0;
}

// comparator for sorting peers by timestamp, most recent first
inline static int _peerTimestampCompare(const void *peer, const void *otherPeer)
{
    if (((const BRPeer *)peer)->timestamp < ((const BRPeer *)otherPeer)->timestamp) return 1;
    if (((const BRPeer *)peer)->timestamp > ((const BRPeer *)otherPeer)->timestamp) return -1;
    return 0;
}

// returns a hash value for a block's prevBlock value suitable for use in a hashtable
inline static size_t _BRPrevBlockHash(const void *block)
{
    return (size_t)((const BRMerkleBlock *)block)->prevBlock.u32[0];
}

// true if block and otherBlock have equal prevBlock values
inline static int _BRPrevBlockEq(const void *block, const void *otherBlock)
{
    return UInt256Eq(((const BRMerkleBlock *)block)->prevBlock, ((const BRMerkleBlock *)otherBlock)->prevBlock);
}

// returns a hash value for a block's height value suitable for use in a hashtable
inline static size_t _BRBlockHeightHash(const void *block)
{
    // (FNV_OFFSET xor height)*FNV_PRIME
    return (size_t)((0x811C9dc5 ^ ((const BRMerkleBlock *)block)->height)*0x01000193);
}

// true if block and otherBlock have equal height values
inline static int _BRBlockHeightEq(const void *block, const void *otherBlock)
{
    return (((const BRMerkleBlock *)block)->height == ((const BRMerkleBlock *)otherBlock)->height);
}

struct BRPeerManagerStruct {
    BRWallet *wallet;
    int isConnected, connectFailureCount, misbehavinCount, dnsThreadCount, maxConnectCount;
    BRPeer *peers, *downloadPeer, fixedPeer, **connectedPeers;
    char downloadPeerName[INET6_ADDRSTRLEN + 6];
    uint32_t earliestKeyTime, syncStartHeight, filterUpdateHeight, estimatedHeight;
    BRBloomFilter *bloomFilter;
    double fpRate, averageTxPerBlock;
    BRSet *blocks, *orphans, *checkpoints;
    BRMerkleBlock *lastBlock, *lastOrphan;
    BRTxPeerList *txRelays, *txRequests;
    BRPublishedTx *publishedTx;
    UInt256 *publishedTxHashes;
    void *info;
    void (*syncStarted)(void *info);
    void (*syncStopped)(void *info, int error);
    void (*txStatusUpdate)(void *info);
    void (*saveBlocks)(void *info, int replace, BRMerkleBlock *blocks[], size_t blocksCount);
    void (*savePeers)(void *info, int replace, const BRPeer peers[], size_t peersCount);
    int (*networkIsReachable)(void *info);
    void (*threadCleanup)(void *info);
    pthread_mutex_t lock;
};

static void _BRPeerManagerPeerMisbehavin(BRPeerManager *manager, BRPeer *peer)
{
    for (size_t i = array_count(manager->peers); i > 0; i--) {
        if (BRPeerEq(&manager->peers[i - 1], peer)) array_rm(manager->peers, i - 1);
    }

    if (++manager->misbehavinCount >= 10) { // clear out stored peers so we get a fresh list from DNS for next connect
        manager->misbehavinCount = 0;
        array_clear(manager->peers);
    }

    BRPeerDisconnect(peer);
}

static void _BRPeerManagerSyncStopped(BRPeerManager *manager)
{
    manager->syncStartHeight = 0;

    if (manager->downloadPeer) {
        // don't cancel timeout if there's a pending tx publish callback
        for (size_t i = array_count(manager->publishedTx); i > 0; i--) {
            if (manager->publishedTx[i - 1].callback != NULL) return;
        }
    
        BRPeerScheduleDisconnect(manager->downloadPeer, -1); // cancel sync timeout
    }
}

// adds transaction to list of tx to be published, along with any unconfirmed inputs
static void _BRPeerManagerAddTxToPublishList(BRPeerManager *manager, BRTransaction *tx, void *info,
                                             void (*callback)(void *, int))
{
    if (tx && tx->blockHeight == TX_UNCONFIRMED) {
        for (size_t i = array_count(manager->publishedTx); i > 0; i--) {
            if (BRTransactionEq(manager->publishedTx[i - 1].tx, tx)) return;
        }
        
        array_add(manager->publishedTx, ((BRPublishedTx) { tx, info, callback }));
        array_add(manager->publishedTxHashes, tx->txHash);

        for (size_t i = 0; i < tx->inCount; i++) {
            _BRPeerManagerAddTxToPublishList(manager, BRWalletTransactionForHash(manager->wallet, tx->inputs[i].txHash),
                                             NULL, NULL);
        }
    }
}

static size_t _BRPeerManagerBlockLocators(BRPeerManager *manager, UInt256 locators[], size_t locatorsCount)
{
    // append 10 most recent block hashes, decending, then continue appending, doubling the step back each time,
    // finishing with the genesis block (top, -1, -2, -3, -4, -5, -6, -7, -8, -9, -11, -15, -23, -39, -71, -135, ..., 0)
    BRMerkleBlock *block = manager->lastBlock;
    int32_t step = 1, i = 0, j;
    
    while (block && block->height > 0) {
        if (locators && i < locatorsCount) locators[i] = block->blockHash;
        if (++i >= 10) step *= 2;
        
        for (j = 0; block && j < step; j++) {
            block = BRSetGet(manager->blocks, &block->prevBlock);
        }
    }
    
    if (locators && i < locatorsCount) locators[i] = GENESIS_BLOCK_HASH;
    return ++i;
}

static void _setApplyFreeBlock(void *info, void *block)
{
    BRMerkleBlockFree(block);
}

static void _BRPeerManagerLoadBloomFilter(BRPeerManager *manager, BRPeer *peer)
{
    // every time a new wallet address is added, the bloom filter has to be rebuilt, and each address is only used
    // for one transaction, so here we generate some spare addresses to avoid rebuilding the filter each time a
    // wallet transaction is encountered during the chain sync
    BRWalletUnusedAddrs(manager->wallet, NULL, SEQUENCE_GAP_LIMIT_EXTERNAL + 100, 0);
    BRWalletUnusedAddrs(manager->wallet, NULL, SEQUENCE_GAP_LIMIT_INTERNAL + 100, 1);

    BRSetApply(manager->orphans, NULL, _setApplyFreeBlock);
    BRSetClear(manager->orphans); // clear out orphans that may have been received on an old filter
    manager->lastOrphan = NULL;
    manager->filterUpdateHeight = manager->lastBlock->height;
    manager->fpRate = BLOOM_REDUCED_FALSEPOSITIVE_RATE;
    
    size_t addrsCount = BRWalletAllAddrs(manager->wallet, NULL, 0);
    BRAddress *addrs = malloc(addrsCount*sizeof(*addrs));
    size_t utxosCount = BRWalletUTXOs(manager->wallet, NULL, 0);
    BRUTXO *utxos = malloc(utxosCount*sizeof(*utxos));
    uint32_t blockHeight = (manager->lastBlock->height > 100) ? manager->lastBlock->height - 100 : 0;
    size_t txCount = BRWalletTxUnconfirmedBefore(manager->wallet, NULL, 0, blockHeight);
    BRTransaction **transactions = malloc(txCount*sizeof(*transactions));
    BRBloomFilter *filter;
    
    assert(addrs != NULL);
    assert(utxos != NULL);
    assert(transactions != NULL);
    addrsCount = BRWalletAllAddrs(manager->wallet, addrs, addrsCount);
    utxosCount = BRWalletUTXOs(manager->wallet, utxos, utxosCount);
    txCount = BRWalletTxUnconfirmedBefore(manager->wallet, transactions, txCount, blockHeight);
    filter = BRBloomFilterNew(manager->fpRate, addrsCount + utxosCount + txCount + 100, (uint32_t)BRPeerHash(peer),
                              BLOOM_UPDATE_ALL); // BUG: XXX txCount not the same as number of spent wallet outputs
    
    for (size_t i = 0; i < addrsCount; i++) { // add addresses to watch for tx receiveing money to the wallet
        UInt160 hash = UINT160_ZERO;
        
        BRAddressHash160(&hash, addrs[i].s);
        
        if (! UInt160IsZero(hash) && ! BRBloomFilterContainsData(filter, hash.u8, sizeof(hash))) {
            BRBloomFilterInsertData(filter, hash.u8, sizeof(hash));
        }
    }

    free(addrs);
        
    for (size_t i = 0; i < utxosCount; i++) { // add UTXOs to watch for tx sending money from the wallet
        uint8_t o[sizeof(UInt256) + sizeof(uint32_t)];
        
        UInt256Set(o, utxos[i].hash);
        UInt32SetLE(&o[sizeof(UInt256)], utxos[i].n);
        if (! BRBloomFilterContainsData(filter, o, sizeof(o))) BRBloomFilterInsertData(filter, o, sizeof(o));
    }
    
    free(utxos);
        
    for (size_t i = 0; i < txCount; i++) { // also add TXOs spent within the last 100 blocks
        for (size_t j = 0; j < transactions[i]->inCount; j++) {
            BRTxInput *input = &transactions[i]->inputs[j];
            BRTransaction *tx = BRWalletTransactionForHash(manager->wallet, input->txHash);
            uint8_t o[sizeof(UInt256) + sizeof(uint32_t)];
            
            if (tx && input->index < tx->outCount &&
                BRWalletContainsAddress(manager->wallet, tx->outputs[input->index].address)) {
                UInt256Set(o, input->txHash);
                UInt32SetLE(&o[sizeof(UInt256)], input->index);
                if (! BRBloomFilterContainsData(filter, o, sizeof(o))) BRBloomFilterInsertData(filter, o,sizeof(o));
            }
        }
    }
    
    free(transactions);
    if (manager->bloomFilter) BRBloomFilterFree(manager->bloomFilter);
    manager->bloomFilter = filter;
    // TODO: XXX if already synced, recursively add inputs of unconfirmed receives

    uint8_t data[BRBloomFilterSerialize(filter, NULL, 0)];
    size_t len = BRBloomFilterSerialize(filter, data, sizeof(data));
    
    BRPeerSendFilterload(peer, data, len);
}

static void _updateFilterRerequestDone(void *info, int success)
{
    BRPeer *peer = ((BRPeerCallbackInfo *)info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *)info)->manager;
    
    free(info);
    
    if (success) {
        pthread_mutex_lock(&manager->lock);

        if ((peer->flags & PEER_FLAG_NEEDSUPDATE) == 0) {
            UInt256 locators[_BRPeerManagerBlockLocators(manager, NULL, 0)];
            size_t count = _BRPeerManagerBlockLocators(manager, locators, sizeof(locators)/sizeof(*locators));
            
            BRPeerSendGetblocks(peer, locators, count, UINT256_ZERO);
        }

        pthread_mutex_unlock(&manager->lock);
    }
}

static void _updateFilterLoadDone(void *info, int success)
{
    BRPeer *peer = ((BRPeerCallbackInfo *)info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *)info)->manager;
    BRPeerCallbackInfo *peerInfo;

    free(info);
    
    if (success) {
        pthread_mutex_lock(&manager->lock);
        BRPeerSetNeedsFilterUpdate(peer, 0);
        peer->flags &= ~PEER_FLAG_NEEDSUPDATE;
        
        if (manager->lastBlock->height < manager->estimatedHeight) { // if syncing, rerequest blocks
            peerInfo = calloc(1, sizeof(*peerInfo));
            assert(peerInfo != NULL);
            peerInfo->peer = peer;
            peerInfo->manager = manager;
            BRPeerRerequestBlocks(manager->downloadPeer, manager->lastBlock->blockHash);
            BRPeerSendPing(manager->downloadPeer, peerInfo, _updateFilterRerequestDone);
        }
        else BRPeerSendMempool(peer, NULL, 0, NULL, NULL); // if not syncing, request mempool
        
        pthread_mutex_unlock(&manager->lock);
    }
}

static void _updateFilterPingDone(void *info, int success)
{
    BRPeer *peer = ((BRPeerCallbackInfo *)info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *)info)->manager;
    BRPeerCallbackInfo *peerInfo;
    
    if (success) {
        pthread_mutex_lock(&manager->lock);
        peer_log(peer, "updating filter with newly created wallet addresses");
        if (manager->bloomFilter) BRBloomFilterFree(manager->bloomFilter);
        manager->bloomFilter = NULL;

        if (manager->lastBlock->height < manager->estimatedHeight) { // if we're syncing, only update download peer
            if (manager->downloadPeer) {
                _BRPeerManagerLoadBloomFilter(manager, manager->downloadPeer);
                BRPeerSendPing(manager->downloadPeer, info, _updateFilterLoadDone); // wait for pong so filter is loaded
            }
            else free(info);
        }
        else {
            free(info);
            
            for (size_t i = array_count(manager->connectedPeers); i > 0; i--) {
                if (BRPeerConnectStatus(manager->connectedPeers[i - 1]) != BRPeerStatusConnected) continue;
                peerInfo = calloc(1, sizeof(*peerInfo));
                assert(peerInfo != NULL);
                peerInfo->peer = manager->connectedPeers[i - 1];
                peerInfo->manager = manager;
                _BRPeerManagerLoadBloomFilter(manager, peerInfo->peer);
                BRPeerSendPing(peerInfo->peer, peerInfo, _updateFilterLoadDone); // wait for pong so filter is loaded
            }
        }

         pthread_mutex_unlock(&manager->lock);
    }
    else free(info);
}

static void _BRPeerManagerUpdateFilter(BRPeerManager *manager)
{
    BRPeerCallbackInfo *info;

    if (manager->downloadPeer && (manager->downloadPeer->flags & PEER_FLAG_NEEDSUPDATE) == 0) {
        BRPeerSetNeedsFilterUpdate(manager->downloadPeer, 1);
        manager->downloadPeer->flags |= PEER_FLAG_NEEDSUPDATE;
        peer_log(manager->downloadPeer, "filter update needed, waiting for pong");
        info = calloc(1, sizeof(*info));
        assert(info != NULL);
        info->peer = manager->downloadPeer;
        info->manager = manager;
        // wait for pong so we're sure to include any tx already sent by the peer in the updated filter
        BRPeerSendPing(manager->downloadPeer, info, _updateFilterPingDone);
    }
}

static void _BRPeerManagerUpdateTx(BRPeerManager *manager, const UInt256 txHashes[], size_t txCount,
                                   uint32_t blockHeight, uint32_t timestamp)
{
    if (blockHeight != TX_UNCONFIRMED) { // remove confirmed tx from publish list and relay counts
        for (size_t i = 0; i < txCount; i++) {
            for (size_t j = array_count(manager->publishedTx); j > 0; j--) {
                BRTransaction *tx = manager->publishedTx[j - 1].tx;
                
                if (! UInt256Eq(txHashes[i], tx->txHash)) continue;
                array_rm(manager->publishedTx, j - 1);
                array_rm(manager->publishedTxHashes, j - 1);
                if (! BRWalletTransactionForHash(manager->wallet, tx->txHash)) BRTransactionFree(tx);
            }
            
            for (size_t j = array_count(manager->txRelays); j > 0; j--) {
                if (! UInt256Eq(txHashes[i], manager->txRelays[j - 1].txHash)) continue;
                array_free(manager->txRelays[j - 1].peers);
                array_rm(manager->txRelays, j - 1);
            }
        }
    }
    
    BRWalletUpdateTransactions(manager->wallet, txHashes, txCount, blockHeight, timestamp);
}

// unconfirmed transactions that aren't in the mempools of any of connected peers have likely dropped off the network
static void _requestUnrelayedTxGetdataDone(void *info, int success)
{
    BRPeer *peer = ((BRPeerCallbackInfo *)info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *)info)->manager;
    int isPublishing;
    size_t count = 0;

    free(info);
    pthread_mutex_lock(&manager->lock);
    if (success) peer->flags |= PEER_FLAG_SYNCED;
    
    for (size_t i = array_count(manager->connectedPeers); i > 0; i--) {
        peer = manager->connectedPeers[i - 1];
        if (BRPeerConnectStatus(peer) == BRPeerStatusConnected) count++;
        if ((peer->flags & PEER_FLAG_SYNCED) != 0) continue;
        count = 0;
        break;
    }

    // don't remove transactions until we're connected to maxConnectCount peers, and all peers have finished
    // relaying their mempools
    if (count >= manager->maxConnectCount) {
        size_t txCount = BRWalletTxUnconfirmedBefore(manager->wallet, NULL, 0, TX_UNCONFIRMED);
        BRTransaction *tx[(txCount < 10000) ? txCount : 10000];
        
        txCount = BRWalletTxUnconfirmedBefore(manager->wallet, tx, sizeof(tx)/sizeof(*tx), TX_UNCONFIRMED);

        for (size_t i = 0; i < txCount; i++) {
            isPublishing = 0;
            
            for (size_t j = array_count(manager->publishedTx); ! isPublishing && j > 0; j--) {
                if (BRTransactionEq(manager->publishedTx[j - 1].tx, tx[i]) &&
                    manager->publishedTx[j - 1].callback != NULL) isPublishing = 1;
            }
            
            if (! isPublishing && _BRTxPeerListCount(manager->txRelays, tx[i]->txHash) == 0 &&
                _BRTxPeerListCount(manager->txRequests, tx[i]->txHash) == 0) {
                BRWalletRemoveTransaction(manager->wallet, tx[i]->txHash);
            }
            else if (! isPublishing && _BRTxPeerListCount(manager->txRelays, tx[i]->txHash) < manager->maxConnectCount){
                // set timestamp 0 to mark as unverified
                _BRPeerManagerUpdateTx(manager, &tx[i]->txHash, 1, TX_UNCONFIRMED, 0);
            }
        }
    }

    pthread_mutex_unlock(&manager->lock);
}

static void _BRPeerManagerRequestUnrelayedTx(BRPeerManager *manager, BRPeer *peer)
{
    BRPeerCallbackInfo *info;
    size_t hashCount = 0, txCount = BRWalletTxUnconfirmedBefore(manager->wallet, NULL, 0, TX_UNCONFIRMED);
    BRTransaction *tx[txCount];
    UInt256 txHashes[txCount];
    
    txCount = BRWalletTxUnconfirmedBefore(manager->wallet, tx, txCount, TX_UNCONFIRMED);
    
    for (size_t i = 0; i < txCount; i++) {
        if (! _BRTxPeerListHasPeer(manager->txRelays, tx[i]->txHash, peer) &&
            ! _BRTxPeerListHasPeer(manager->txRequests, tx[i]->txHash, peer)) {
            txHashes[hashCount++] = tx[i]->txHash;
            _BRTxPeerListAddPeer(&manager->txRequests, tx[i]->txHash, peer);
        }
    }

    if (hashCount > 0) {
        BRPeerSendGetdata(peer, txHashes, hashCount, NULL, 0);
    
        if ((peer->flags & PEER_FLAG_SYNCED) == 0) {
            info = calloc(1, sizeof(*info));
            assert(info != NULL);
            info->peer = peer;
            info->manager = manager;
            BRPeerSendPing(peer, info, _requestUnrelayedTxGetdataDone);
        }
    }
    else peer->flags |= PEER_FLAG_SYNCED;
}

static void _BRPeerManagerPublishPendingTx(BRPeerManager *manager, BRPeer *peer)
{
    for (size_t i = array_count(manager->publishedTx); i > 0; i--) {
        if (manager->publishedTx[i - 1].callback == NULL) continue;
        BRPeerScheduleDisconnect(peer, PROTOCOL_TIMEOUT); // schedule publish timeout
        break;
    }
    
    BRPeerSendInv(peer, manager->publishedTxHashes, array_count(manager->publishedTxHashes));
}

static void _mempoolDone(void *info, int success)
{
    BRPeer *peer = ((BRPeerCallbackInfo *)info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *)info)->manager;
    int syncFinished = 0;
    
    free(info);
    
    if (success) {
        peer_log(peer, "mempool request finished");
        pthread_mutex_lock(&manager->lock);
        if (manager->syncStartHeight > 0) {
            peer_log(peer, "sync succeeded");
            syncFinished = 1;
            _BRPeerManagerSyncStopped(manager);
        }

        _BRPeerManagerRequestUnrelayedTx(manager, peer);
        BRPeerSendGetaddr(peer); // request a list of other bitcoin peers
        pthread_mutex_unlock(&manager->lock);
        if (manager->txStatusUpdate) manager->txStatusUpdate(manager->info);
        if (syncFinished && manager->syncStopped) manager->syncStopped(manager->info, 0);
    }
    else peer_log(peer, "mempool request failed");
}

static void _loadBloomFilterDone(void *info, int success)
{
    BRPeer *peer = ((BRPeerCallbackInfo *)info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *)info)->manager;

    pthread_mutex_lock(&manager->lock);
    
    if (success) {
        BRPeerSendMempool(peer, manager->publishedTxHashes, array_count(manager->publishedTxHashes), info,
                          _mempoolDone);
        pthread_mutex_unlock(&manager->lock);
    }
    else {
        free(info);
        
        if (peer == manager->downloadPeer) {
            peer_log(peer, "sync succeeded");
            _BRPeerManagerSyncStopped(manager);
            pthread_mutex_unlock(&manager->lock);
            if (manager->syncStopped) manager->syncStopped(manager->info, 0);
        }
        else pthread_mutex_unlock(&manager->lock);
    }
}

static void _BRPeerManagerLoadMempools(BRPeerManager *manager)
{
    // after syncing, load filters and get mempools from other peers
    for (size_t i = array_count(manager->connectedPeers); i > 0; i--) {
        BRPeer *peer = manager->connectedPeers[i - 1];
        BRPeerCallbackInfo *info;

        if (BRPeerConnectStatus(peer) != BRPeerStatusConnected) continue;
        info = calloc(1, sizeof(*info));
        assert(info != NULL);
        info->peer = peer;
        info->manager = manager;
        
        if (peer != manager->downloadPeer || manager->fpRate > BLOOM_REDUCED_FALSEPOSITIVE_RATE*5.0) {
            _BRPeerManagerLoadBloomFilter(manager, peer);
            _BRPeerManagerPublishPendingTx(manager, peer);
            BRPeerSendPing(peer, info, _loadBloomFilterDone); // load mempool after updating bloomfilter
        }
        else BRPeerSendMempool(peer, manager->publishedTxHashes, array_count(manager->publishedTxHashes), info,
                               _mempoolDone);
    }
}

// returns a UINT128_ZERO terminated array of addresses for hostname that must be freed, or NULL if lookup failed
static UInt128 *_addressLookup(const char *hostname)
{
    struct addrinfo *servinfo, *p;
    UInt128 *addrList = NULL;
    size_t count = 0, i = 0;
    
    if (getaddrinfo(hostname, NULL, NULL, &servinfo) == 0) {
        for (p = servinfo; p != NULL; p = p->ai_next) count++;
        if (count > 0) addrList = calloc(count + 1, sizeof(*addrList));
        assert(addrList != NULL || count == 0);
        
        for (p = servinfo; p != NULL; p = p->ai_next) {
            if (p->ai_family == AF_INET) {
                addrList[i].u16[5] = 0xffff;
                addrList[i].u32[3] = ((struct sockaddr_in *)p->ai_addr)->sin_addr.s_addr;
                i++;
            }
            else if (p->ai_family == AF_INET6) {
                addrList[i++] = *(UInt128 *)&((struct sockaddr_in6 *)p->ai_addr)->sin6_addr;
            }
        }
        
        freeaddrinfo(servinfo);
    }
    
    return addrList;
}

static void *_findPeersThreadRoutine(void *arg)
{
    BRPeerManager *manager = ((BRFindPeersInfo *)arg)->manager;
    uint64_t services = ((BRFindPeersInfo *)arg)->services;
    UInt128 *addrList, *addr;
    time_t now = time(NULL), age;
    
    pthread_cleanup_push(manager->threadCleanup, manager->info);
    addrList = _addressLookup(((BRFindPeersInfo *)arg)->hostname);
    free(arg);
    pthread_mutex_lock(&manager->lock);
    
    for (addr = addrList; addr && ! UInt128IsZero(*addr); addr++) {
        age = 24*60*60 + BRRand(2*24*60*60); // add between 1 and 3 days
        array_add(manager->peers, ((BRPeer) { *addr, STANDARD_PORT, services, now - age, 0 }));
    }

    manager->dnsThreadCount--;
    pthread_mutex_unlock(&manager->lock);
    if (addrList) free(addrList);
    pthread_cleanup_pop(1);
    return NULL;
}

// DNS peer discovery
static void _BRPeerManagerFindPeers(BRPeerManager *manager)
{
    static const uint64_t services = SERVICES_NODE_NETWORK | SERVICES_NODE_BLOOM;
    time_t now = time(NULL);
    struct timespec ts;
    pthread_t thread;
    pthread_attr_t attr;
    UInt128 *addr, *addrList;
    BRFindPeersInfo *info;
    
    if (! UInt128IsZero(manager->fixedPeer.address)) {
        array_set_count(manager->peers, 1);
        manager->peers[0] = manager->fixedPeer;
        manager->peers[0].services = services;
        manager->peers[0].timestamp = now;
    }
    else {
        for (size_t i = 1; i < DNS_SEEDS_COUNT; i++) {
            info = calloc(1, sizeof(BRFindPeersInfo));
            assert(info != NULL);
            info->manager = manager;
            info->hostname = dns_seeds[i];
            info->services = services;
            if (pthread_attr_init(&attr) == 0 && pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED) == 0 &&
                pthread_create(&thread, &attr, _findPeersThreadRoutine, info) == 0) manager->dnsThreadCount++;
        }

        for (addr = addrList = _addressLookup(dns_seeds[0]); addr && ! UInt128IsZero(*addr); addr++) {
            array_add(manager->peers, ((BRPeer) { *addr, STANDARD_PORT, services, now, 0 }));
        }

        if (addrList) free(addrList);
        ts.tv_sec = 0;
        ts.tv_nsec = 1;

        do {
            pthread_mutex_unlock(&manager->lock);
            nanosleep(&ts, NULL); // pthread_yield() isn't POSIX standard :(
            pthread_mutex_lock(&manager->lock);
        } while (manager->dnsThreadCount > 0 && array_count(manager->peers) < PEER_MAX_CONNECTIONS);
    
        qsort(manager->peers, array_count(manager->peers), sizeof(*manager->peers), _peerTimestampCompare);
    }
}

static void _peerConnected(void *info)
{
    BRPeer *peer = ((BRPeerCallbackInfo *)info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *)info)->manager;
    BRPeerCallbackInfo *peerInfo;
    time_t now = time(NULL);
    
    pthread_mutex_lock(&manager->lock);
    if (peer->timestamp > now + 2*60*60 || peer->timestamp < now - 2*60*60) peer->timestamp = now; // sanity check
    
    // TODO: XXX does this work with 0.11 pruned nodes?
    if (! (peer->services & SERVICES_NODE_NETWORK)) {
        peer_log(peer, "node doesn't carry full blocks");
        BRPeerDisconnect(peer);
    }
    else if (BRPeerLastBlock(peer) + 10 < manager->lastBlock->height) {
        peer_log(peer, "node isn't synced");
        BRPeerDisconnect(peer);
    }
    else if ((peer->services & SERVICES_NODE_BCASH) == SERVICES_NODE_BCASH) {
        peer_log(peer, "b-cash nodes not supported");
        BRPeerDisconnect(peer);
    }
    else if (BRPeerVersion(peer) >= 70011 && ! (peer->services & SERVICES_NODE_BLOOM)) {
        peer_log(peer, "node doesn't support SPV mode");
        BRPeerDisconnect(peer);
    }
    else if (manager->downloadPeer && // check if we should stick with the existing download peer
             (BRPeerLastBlock(manager->downloadPeer) >= BRPeerLastBlock(peer) ||
              manager->lastBlock->height >= BRPeerLastBlock(peer))) {
        if (manager->lastBlock->height >= BRPeerLastBlock(peer)) { // only load bloom filter if we're done syncing
            manager->connectFailureCount = 0; // also reset connect failure count if we're already synced
            _BRPeerManagerLoadBloomFilter(manager, peer);
            _BRPeerManagerPublishPendingTx(manager, peer);
            peerInfo = calloc(1, sizeof(*peerInfo));
            assert(peerInfo != NULL);
            peerInfo->peer = peer;
            peerInfo->manager = manager;
            BRPeerSendPing(peer, peerInfo, _loadBloomFilterDone);
        }
    }
    else { // select the peer with the lowest ping time to download the chain from if we're behind
        // BUG: XXX a malicious peer can report a higher lastblock to make us select them as the download peer, if
        // two peers agree on lastblock, use one of those two instead
        for (size_t i = array_count(manager->connectedPeers); i > 0; i--) {
            BRPeer *p = manager->connectedPeers[i - 1];
            
            if (BRPeerConnectStatus(p) != BRPeerStatusConnected) continue;
            if ((BRPeerPingTime(p) < BRPeerPingTime(peer) && BRPeerLastBlock(p) >= BRPeerLastBlock(peer)) ||
                BRPeerLastBlock(p) > BRPeerLastBlock(peer)) peer = p;
        }
        
        if (manager->downloadPeer) BRPeerDisconnect(manager->downloadPeer);
        manager->downloadPeer = peer;
        manager->isConnected = 1;
        manager->estimatedHeight = BRPeerLastBlock(peer);
        _BRPeerManagerLoadBloomFilter(manager, peer);
        BRPeerSetCurrentBlockHeight(peer, manager->lastBlock->height);
        _BRPeerManagerPublishPendingTx(manager, peer);
            
        if (manager->lastBlock->height < BRPeerLastBlock(peer)) { // start blockchain sync
            UInt256 locators[_BRPeerManagerBlockLocators(manager, NULL, 0)];
            size_t count = _BRPeerManagerBlockLocators(manager, locators, sizeof(locators)/sizeof(*locators));
            
            BRPeerScheduleDisconnect(peer, PROTOCOL_TIMEOUT); // schedule sync timeout

            // request just block headers up to a week before earliestKeyTime, and then merkleblocks after that
            // we do not reset connect failure count yet incase this request times out
            if (manager->lastBlock->timestamp + 7*24*60*60 >= manager->earliestKeyTime) {
                BRPeerSendGetblocks(peer, locators, count, UINT256_ZERO);
            }
            else BRPeerSendGetheaders(peer, locators, count, UINT256_ZERO);
        }
        else { // we're already synced
            manager->connectFailureCount = 0; // reset connect failure count
            _BRPeerManagerLoadMempools(manager);
        }
    }

    pthread_mutex_unlock(&manager->lock);
}

static void _peerDisconnected(void *info, int error)
{
    BRPeer *peer = ((BRPeerCallbackInfo *)info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *)info)->manager;
    BRTxPeerList *peerList;
    int willSave = 0, willReconnect = 0, txError = 0;
    size_t txCount = 0;
    
    //free(info);
    pthread_mutex_lock(&manager->lock);

    void *txInfo[array_count(manager->publishedTx)];
    void (*txCallback[array_count(manager->publishedTx)])(void *, int);
    
    if (error == EPROTO) { // if it's protocol error, the peer isn't following standard policy
        _BRPeerManagerPeerMisbehavin(manager, peer);
    }
    else if (error) { // timeout or some non-protocol related network error
        for (size_t i = array_count(manager->peers); i > 0; i--) {
            if (BRPeerEq(&manager->peers[i - 1], peer)) array_rm(manager->peers, i - 1);
        }
        
        manager->connectFailureCount++;
        
        // if it's a timeout and there's pending tx publish callbacks, the tx publish timed out
        // BUG: XXX what if it's a connect timeout and not a publish timeout?
        if (error == ETIMEDOUT && (peer != manager->downloadPeer || manager->syncStartHeight == 0 ||
                                   array_count(manager->connectedPeers) == 1)) txError = ETIMEDOUT;
    }
    
    for (size_t i = array_count(manager->txRelays); i > 0; i--) {
        peerList = &manager->txRelays[i - 1];

        for (size_t j = array_count(peerList->peers); j > 0; j--) {
            if (BRPeerEq(&peerList->peers[j - 1], peer)) array_rm(peerList->peers, j - 1);
        }
    }

    if (peer == manager->downloadPeer) { // download peer disconnected
        manager->isConnected = 0;
        manager->downloadPeer = NULL;
        if (manager->connectFailureCount > MAX_CONNECT_FAILURES) manager->connectFailureCount = MAX_CONNECT_FAILURES;
    }

    if (! manager->isConnected && manager->connectFailureCount == MAX_CONNECT_FAILURES) {
        _BRPeerManagerSyncStopped(manager);
        
        // clear out stored peers so we get a fresh list from DNS on next connect attempt
        array_clear(manager->peers);
        txError = ENOTCONN; // trigger any pending tx publish callbacks
        willSave = 1;
        peer_log(peer, "sync failed");
    }
    else if (manager->connectFailureCount < MAX_CONNECT_FAILURES) willReconnect = 1;
    
    if (txError) {
        for (size_t i = array_count(manager->publishedTx); i > 0; i--) {
            if (manager->publishedTx[i - 1].callback == NULL) continue;
            peer_log(peer, "transaction canceled: %s", strerror(txError));
            txInfo[txCount] = manager->publishedTx[i - 1].info;
            txCallback[txCount] = manager->publishedTx[i - 1].callback;
            txCount++;
            BRTransactionFree(manager->publishedTx[i - 1].tx);
            array_rm(manager->publishedTxHashes, i - 1);
            array_rm(manager->publishedTx, i - 1);
        }
    }
    
    for (size_t i = array_count(manager->connectedPeers); i > 0; i--) {
        if (manager->connectedPeers[i - 1] != peer) continue;
        array_rm(manager->connectedPeers, i - 1);
        break;
    }

    BRPeerFree(peer);
    pthread_mutex_unlock(&manager->lock);
    
    for (size_t i = 0; i < txCount; i++) {
        txCallback[i](txInfo[i], txError);
    }
    
    if (willSave && manager->savePeers) manager->savePeers(manager->info, 1, NULL, 0);
    if (willSave && manager->syncStopped) manager->syncStopped(manager->info, error);
    if (willReconnect) BRPeerManagerConnect(manager); // try connecting to another peer
    if (manager->txStatusUpdate) manager->txStatusUpdate(manager->info);
}

static void _peerRelayedPeers(void *info, const BRPeer peers[], size_t peersCount)
{
    BRPeer *peer = ((BRPeerCallbackInfo *)info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *)info)->manager;
    time_t now = time(NULL);

    pthread_mutex_lock(&manager->lock);
    peer_log(peer, "relayed %zu peer(s)", peersCount);

    array_add_array(manager->peers, peers, peersCount);
    qsort(manager->peers, array_count(manager->peers), sizeof(*manager->peers), _peerTimestampCompare);

    // limit total to 2500 peers
    if (array_count(manager->peers) > 2500) array_set_count(manager->peers, 2500);
    peersCount = array_count(manager->peers);
    
    // remove peers more than 3 hours old, or until there are only 1000 left
    while (peersCount > 1000 && manager->peers[peersCount - 1].timestamp + 3*60*60 < now) peersCount--;
    array_set_count(manager->peers, peersCount);
    
    BRPeer save[peersCount];

    for (size_t i = 0; i < peersCount; i++) save[i] = manager->peers[i];
    pthread_mutex_unlock(&manager->lock);
    
    // peer relaying is complete when we receive <1000
    if (peersCount > 1 && peersCount < 1000 &&
        manager->savePeers) manager->savePeers(manager->info, 1, save, peersCount);
}

static void _peerRelayedTx(void *info, BRTransaction *tx)
{
    BRPeer *peer = ((BRPeerCallbackInfo *)info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *)info)->manager;
    void *txInfo = NULL;
    void (*txCallback)(void *, int) = NULL;
    int isWalletTx = 0, hasPendingCallbacks = 0;
    size_t relayCount = 0;
    
    pthread_mutex_lock(&manager->lock);
    peer_log(peer, "relayed tx: %s", u256_hex_encode(tx->txHash));
    
    for (size_t i = array_count(manager->publishedTx); i > 0; i--) { // see if tx is in list of published tx
        if (UInt256Eq(manager->publishedTxHashes[i - 1], tx->txHash)) {
            txInfo = manager->publishedTx[i - 1].info;
            txCallback = manager->publishedTx[i - 1].callback;
            manager->publishedTx[i - 1].info = NULL;
            manager->publishedTx[i - 1].callback = NULL;
            relayCount = _BRTxPeerListAddPeer(&manager->txRelays, tx->txHash, peer);
        }
        else if (manager->publishedTx[i - 1].callback != NULL) hasPendingCallbacks = 1;
    }

    // cancel tx publish timeout if no publish callbacks are pending, and syncing is done or this is not downloadPeer
    if (! hasPendingCallbacks && (manager->syncStartHeight == 0 || peer != manager->downloadPeer)) {
        BRPeerScheduleDisconnect(peer, -1); // cancel publish tx timeout
    }

    if (manager->syncStartHeight == 0 || BRWalletContainsTransaction(manager->wallet, tx)) {
        isWalletTx = BRWalletRegisterTransaction(manager->wallet, tx);
        if (isWalletTx) tx = BRWalletTransactionForHash(manager->wallet, tx->txHash);
    }
    else {
        BRTransactionFree(tx);
        tx = NULL;
    }
    
    if (tx && isWalletTx) {
        // reschedule sync timeout
        if (manager->syncStartHeight > 0 && peer == manager->downloadPeer) {
            BRPeerScheduleDisconnect(peer, PROTOCOL_TIMEOUT);
        }
        
        if (BRWalletAmountSentByTx(manager->wallet, tx) > 0 && BRWalletTransactionIsValid(manager->wallet, tx)) {
            _BRPeerManagerAddTxToPublishList(manager, tx, NULL, NULL); // add valid send tx to mempool
        }

        // keep track of how many peers have or relay a tx, this indicates how likely the tx is to confirm
        // (we only need to track this after syncing is complete)
        if (manager->syncStartHeight == 0) relayCount = _BRTxPeerListAddPeer(&manager->txRelays, tx->txHash, peer);
        
        _BRTxPeerListRemovePeer(manager->txRequests, tx->txHash, peer);
        
        if (manager->bloomFilter != NULL) { // check if bloom filter is already being updated
            BRAddress addrs[SEQUENCE_GAP_LIMIT_EXTERNAL + SEQUENCE_GAP_LIMIT_INTERNAL];
            UInt160 hash;

            // the transaction likely consumed one or more wallet addresses, so check that at least the next <gap limit>
            // unused addresses are still matched by the bloom filter
            BRWalletUnusedAddrs(manager->wallet, addrs, SEQUENCE_GAP_LIMIT_EXTERNAL, 0);
            BRWalletUnusedAddrs(manager->wallet, addrs + SEQUENCE_GAP_LIMIT_EXTERNAL, SEQUENCE_GAP_LIMIT_INTERNAL, 1);

            for (size_t i = 0; i < SEQUENCE_GAP_LIMIT_EXTERNAL + SEQUENCE_GAP_LIMIT_INTERNAL; i++) {
                if (! BRAddressHash160(&hash, addrs[i].s) ||
                    BRBloomFilterContainsData(manager->bloomFilter, hash.u8, sizeof(hash))) continue;
                if (manager->bloomFilter) BRBloomFilterFree(manager->bloomFilter);
                manager->bloomFilter = NULL; // reset bloom filter so it's recreated with new wallet addresses
                _BRPeerManagerUpdateFilter(manager);
                break;
            }
        }
    }
    
    // set timestamp when tx is verified
    if (tx && relayCount >= manager->maxConnectCount && tx->blockHeight == TX_UNCONFIRMED && tx->timestamp == 0) {
        _BRPeerManagerUpdateTx(manager, &tx->txHash, 1, TX_UNCONFIRMED, (uint32_t)time(NULL));
    }
    
    pthread_mutex_unlock(&manager->lock);
    if (txCallback) txCallback(txInfo, 0);
}

static void _peerHasTx(void *info, UInt256 txHash)
{
    BRPeer *peer = ((BRPeerCallbackInfo *)info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *)info)->manager;
    BRTransaction *tx;
    void *txInfo = NULL;
    void (*txCallback)(void *, int) = NULL;
    int isWalletTx = 0, hasPendingCallbacks = 0;
    size_t relayCount = 0;
    
    pthread_mutex_lock(&manager->lock);
    tx = BRWalletTransactionForHash(manager->wallet, txHash);
    peer_log(peer, "has tx: %s", u256_hex_encode(txHash));

    for (size_t i = array_count(manager->publishedTx); i > 0; i--) { // see if tx is in list of published tx
        if (UInt256Eq(manager->publishedTxHashes[i - 1], txHash)) {
            if (! tx) tx = manager->publishedTx[i - 1].tx;
            txInfo = manager->publishedTx[i - 1].info;
            txCallback = manager->publishedTx[i - 1].callback;
            manager->publishedTx[i - 1].info = NULL;
            manager->publishedTx[i - 1].callback = NULL;
            relayCount = _BRTxPeerListAddPeer(&manager->txRelays, txHash, peer);
        }
        else if (manager->publishedTx[i - 1].callback != NULL) hasPendingCallbacks = 1;
    }
    
    // cancel tx publish timeout if no publish callbacks are pending, and syncing is done or this is not downloadPeer
    if (! hasPendingCallbacks && (manager->syncStartHeight == 0 || peer != manager->downloadPeer)) {
        BRPeerScheduleDisconnect(peer, -1); // cancel publish tx timeout
    }

    if (tx) {
        isWalletTx = BRWalletRegisterTransaction(manager->wallet, tx);
        if (isWalletTx) tx = BRWalletTransactionForHash(manager->wallet, tx->txHash);

        // reschedule sync timeout
        if (manager->syncStartHeight > 0 && peer == manager->downloadPeer && isWalletTx) {
            BRPeerScheduleDisconnect(peer, PROTOCOL_TIMEOUT);
        }
        
        // keep track of how many peers have or relay a tx, this indicates how likely the tx is to confirm
        // (we only need to track this after syncing is complete)
        if (manager->syncStartHeight == 0) relayCount = _BRTxPeerListAddPeer(&manager->txRelays, txHash, peer);

        // set timestamp when tx is verified
        if (relayCount >= manager->maxConnectCount && tx && tx->blockHeight == TX_UNCONFIRMED && tx->timestamp == 0) {
            _BRPeerManagerUpdateTx(manager, &txHash, 1, TX_UNCONFIRMED, (uint32_t)time(NULL));
        }

        _BRTxPeerListRemovePeer(manager->txRequests, txHash, peer);
    }
    
    pthread_mutex_unlock(&manager->lock);
    if (txCallback) txCallback(txInfo, 0);
}

static void _peerRejectedTx(void *info, UInt256 txHash, uint8_t code)
{
    BRPeer *peer = ((BRPeerCallbackInfo *)info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *)info)->manager;
    BRTransaction *tx, *t;

    pthread_mutex_lock(&manager->lock);
    peer_log(peer, "rejected tx: %s", u256_hex_encode(txHash));
    tx = BRWalletTransactionForHash(manager->wallet, txHash);
    _BRTxPeerListRemovePeer(manager->txRequests, txHash, peer);

    if (tx) {
        if (_BRTxPeerListRemovePeer(manager->txRelays, txHash, peer) && tx->blockHeight == TX_UNCONFIRMED) {
            // set timestamp 0 to mark tx as unverified
            _BRPeerManagerUpdateTx(manager, &txHash, 1, TX_UNCONFIRMED, 0);
        }

        // if we get rejected for any reason other than double-spend, the peer is likely misconfigured
        if (code != REJECT_SPENT && BRWalletAmountSentByTx(manager->wallet, tx) > 0) {
            for (size_t i = 0; i < tx->inCount; i++) { // check that all inputs are confirmed before dropping peer
                t = BRWalletTransactionForHash(manager->wallet, tx->inputs[i].txHash);
                if (! t || t->blockHeight != TX_UNCONFIRMED) continue;
                tx = NULL;
                break;
            }
            
            if (tx) _BRPeerManagerPeerMisbehavin(manager, peer);
        }
    }

    pthread_mutex_unlock(&manager->lock);
    if (manager->txStatusUpdate) manager->txStatusUpdate(manager->info);
}

static int _BRPeerManagerVerifyBlock(BRPeerManager *manager, BRMerkleBlock *block, BRMerkleBlock *prev, BRPeer *peer)
{
    uint32_t transitionTime = 0;
    int r = 1;
    
    // check if we hit a difficulty transition, and find previous transition time   TODO: fix difficulty interval for KGW,DGW
    if ((block->height % BLOCK_DIFFICULTY_INTERVAL) == 0) {
        BRMerkleBlock *b = block;
        UInt256 prevBlock;

        for (uint32_t i = 0; b && i < BLOCK_DIFFICULTY_INTERVAL; i++) {
            b = BRSetGet(manager->blocks, &b->prevBlock);
        }

        if (! b) {
            peer_log(peer, "missing previous difficulty tansition time, can't verify blockHash: %s",
                     u256_hex_encode(block->blockHash));
            r = 0;
        }
        else {
            transitionTime = b->timestamp;
            prevBlock = b->prevBlock;
        }
        
        while (b) { // free up some memory
            b = BRSetGet(manager->blocks, &prevBlock);
            if (b) prevBlock = b->prevBlock;

            if (b && (b->height % BLOCK_DIFFICULTY_INTERVAL) != 0) {
                BRSetRemove(manager->blocks, b);
                BRMerkleBlockFree(b);
            }
        }
    }

    // verify block difficulty
    if (r && ! BRMerkleBlockVerifyDifficulty(block, prev, transitionTime)) {
        peer_log(peer, "relayed block with invalid difficulty target %x, blockHash: %s", block->target,
                 u256_hex_encode(block->blockHash));
        r = 0;
    }
    
    if (r) {
        BRMerkleBlock *checkpoint = BRSetGet(manager->checkpoints, block);

        // verify blockchain checkpoints
        if (checkpoint && ! BRMerkleBlockEq(block, checkpoint)) {
            peer_log(peer, "relayed a block that differs from the checkpoint at height %"PRIu32", blockHash: %s, "
                     "expected: %s", block->height, u256_hex_encode(block->blockHash),
                     u256_hex_encode(checkpoint->blockHash));
            r = 0;
        }
    }

    return r;
}

static void _peerRelayedBlock(void *info, BRMerkleBlock *block)
{
    BRPeer *peer = ((BRPeerCallbackInfo *)info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *)info)->manager;
    size_t txCount = BRMerkleBlockTxHashes(block, NULL, 0);
    UInt256 _txHashes[(sizeof(UInt256)*txCount <= 0x1000) ? txCount : 0],
            *txHashes = (sizeof(UInt256)*txCount <= 0x1000) ? _txHashes : malloc(txCount*sizeof(*txHashes));
    size_t i, j, fpCount = 0, saveCount = 0;
    BRMerkleBlock orphan, *b, *b2, *prev, *next = NULL;
    uint32_t txTime = 0;
    
    assert(txHashes != NULL);
    txCount = BRMerkleBlockTxHashes(block, txHashes, txCount);
    pthread_mutex_lock(&manager->lock);
    prev = BRSetGet(manager->blocks, &block->prevBlock);

    if (prev) {
        txTime = block->timestamp/2 + prev->timestamp/2;
        block->height = prev->height + 1;
    }
    
    // track the observed bloom filter false positive rate using a low pass filter to smooth out variance
    if (peer == manager->downloadPeer && block->totalTx > 0) {
        for (i = 0; i < txCount; i++) { // wallet tx are not false-positives
            if (! BRWalletTransactionForHash(manager->wallet, txHashes[i])) fpCount++;
        }
        
        // moving average number of tx-per-block
        manager->averageTxPerBlock = manager->averageTxPerBlock*0.999 + block->totalTx*0.001;
        
        // 1% low pass filter, also weights each block by total transactions, compared to the avarage
        manager->fpRate = manager->fpRate*(1.0 - 0.01*block->totalTx/manager->averageTxPerBlock) +
                          0.01*fpCount/manager->averageTxPerBlock;
        
        // false positive rate sanity check
        if (BRPeerConnectStatus(peer) == BRPeerStatusConnected &&
            manager->fpRate > BLOOM_DEFAULT_FALSEPOSITIVE_RATE*10.0) {
            peer_log(peer, "bloom filter false positive rate %f too high after %"PRIu32" blocks, disconnecting...",
                     manager->fpRate, manager->lastBlock->height + 1 - manager->filterUpdateHeight);
            BRPeerDisconnect(peer);
        }
        else if (manager->lastBlock->height + 500 < BRPeerLastBlock(peer) &&
                 manager->fpRate > BLOOM_REDUCED_FALSEPOSITIVE_RATE*10.0) {
            _BRPeerManagerUpdateFilter(manager); // rebuild bloom filter when it starts to degrade
        }
    }

    // ignore block headers that are newer than one week before earliestKeyTime (it's a header if it has 0 totalTx)
    if (block->totalTx == 0 && block->timestamp + 7*24*60*60 > manager->earliestKeyTime + 2*60*60) {
        BRMerkleBlockFree(block);
        block = NULL;
    }
    else if (manager->bloomFilter == NULL) { // ingore potentially incomplete blocks when a filter update is pending
        BRMerkleBlockFree(block);
        block = NULL;

        if (peer == manager->downloadPeer && manager->lastBlock->height < manager->estimatedHeight) {
            BRPeerScheduleDisconnect(peer, PROTOCOL_TIMEOUT); // reschedule sync timeout
            manager->connectFailureCount = 0; // reset failure count once we know our initial request didn't timeout
        }
    }
    else if (! prev) { // block is an orphan
        peer_log(peer, "relayed orphan block %s, previous %s, last block is %s, height %"PRIu32,
                 u256_hex_encode(block->blockHash), u256_hex_encode(block->prevBlock),
                 u256_hex_encode(manager->lastBlock->blockHash), manager->lastBlock->height);
        
        if (block->timestamp + 7*24*60*60 < time(NULL)) { // ignore orphans older than one week ago
            BRMerkleBlockFree(block);
            block = NULL;
        }
        else {
            // call getblocks, unless we already did with the previous block, or we're still syncing
            if (manager->lastBlock->height >= BRPeerLastBlock(peer) &&
                (! manager->lastOrphan || ! UInt256Eq(manager->lastOrphan->blockHash, block->prevBlock))) {
                UInt256 locators[_BRPeerManagerBlockLocators(manager, NULL, 0)];
                size_t locatorsCount = _BRPeerManagerBlockLocators(manager, locators,
                                                                   sizeof(locators)/sizeof(*locators));
                
                peer_log(peer, "calling getblocks");
                BRPeerSendGetblocks(peer, locators, locatorsCount, UINT256_ZERO);
            }
            
            BRSetAdd(manager->orphans, block); // BUG: limit total orphans to avoid memory exhaustion attack
            manager->lastOrphan = block;
        }
    }
    else if (! _BRPeerManagerVerifyBlock(manager, block, prev, peer)) { // block is invalid
        peer_log(peer, "relayed invalid block");
        BRMerkleBlockFree(block);
        block = NULL;
        _BRPeerManagerPeerMisbehavin(manager, peer);
    }
    else if (UInt256Eq(block->prevBlock, manager->lastBlock->blockHash)) { // new block extends main chain
        if ((block->height % 500) == 0 || txCount > 0 || block->height >= BRPeerLastBlock(peer)) {
            peer_log(peer, "adding block #%"PRIu32", false positive rate: %f", block->height, manager->fpRate);
        }
        
        BRSetAdd(manager->blocks, block);
        manager->lastBlock = block;
        if (txCount > 0) _BRPeerManagerUpdateTx(manager, txHashes, txCount, block->height, txTime);
        if (manager->downloadPeer) BRPeerSetCurrentBlockHeight(manager->downloadPeer, block->height);
            
        if (block->height < manager->estimatedHeight && peer == manager->downloadPeer) {
            BRPeerScheduleDisconnect(peer, PROTOCOL_TIMEOUT); // reschedule sync timeout
            manager->connectFailureCount = 0; // reset failure count once we know our initial request didn't timeout
        }
        
        if ((block->height % BLOCK_DIFFICULTY_INTERVAL) == 0) saveCount = 1; // save transition block immediately
        
        if (block->height == manager->estimatedHeight) { // chain download is complete
            saveCount = (block->height % BLOCK_DIFFICULTY_INTERVAL) + BLOCK_DIFFICULTY_INTERVAL + 1;
            _BRPeerManagerLoadMempools(manager);
        }
    }
    else if (BRSetContains(manager->blocks, block)) { // we already have the block (or at least the header)
        if ((block->height % 500) == 0 || txCount > 0 || block->height >= BRPeerLastBlock(peer)) {
            peer_log(peer, "relayed existing block #%"PRIu32, block->height);
        }
        
        b = manager->lastBlock;
        while (b && b->height > block->height) b = BRSetGet(manager->blocks, &b->prevBlock); // is block in main chain?
        
        if (BRMerkleBlockEq(b, block)) { // if it's not on a fork, set block heights for its transactions
            if (txCount > 0) _BRPeerManagerUpdateTx(manager, txHashes, txCount, block->height, txTime);
            if (block->height == manager->lastBlock->height) manager->lastBlock = block;
        }
        
        b = BRSetAdd(manager->blocks, block);

        if (b != block) {
            if (BRSetGet(manager->orphans, b) == b) BRSetRemove(manager->orphans, b);
            if (manager->lastOrphan == b) manager->lastOrphan = NULL;
            BRMerkleBlockFree(b);
        }
    }
    else if (manager->lastBlock->height < BRPeerLastBlock(peer) &&
             block->height > manager->lastBlock->height + 1) { // special case, new block mined durring rescan
        peer_log(peer, "marking new block #%"PRIu32" as orphan until rescan completes", block->height);
        BRSetAdd(manager->orphans, block); // mark as orphan til we're caught up
        manager->lastOrphan = block;
    }
    else if (block->height <= checkpoint_array[CHECKPOINT_COUNT - 1].height) { // fork is older than last checkpoint
        peer_log(peer, "ignoring block on fork older than most recent checkpoint, block #%"PRIu32", hash: %s",
                 block->height, u256_hex_encode(block->blockHash));
        BRMerkleBlockFree(block);
        block = NULL;
    }
    else { // new block is on a fork
        peer_log(peer, "chain fork reached height %"PRIu32, block->height);
        BRSetAdd(manager->blocks, block);

        if (block->height > manager->lastBlock->height) { // check if fork is now longer than main chain
            b = block;
            b2 = manager->lastBlock;
            
            while (b && b2 && ! BRMerkleBlockEq(b, b2)) { // walk back to where the fork joins the main chain
                b = BRSetGet(manager->blocks, &b->prevBlock);
                if (b && b->height < b2->height) b2 = BRSetGet(manager->blocks, &b2->prevBlock);
            }
            
            peer_log(peer, "reorganizing chain from height %"PRIu32", new height is %"PRIu32, b->height, block->height);
        
            BRWalletSetTxUnconfirmedAfter(manager->wallet, b->height); // mark tx after the join point as unconfirmed

            b = block;
        
            while (b && b2 && b->height > b2->height) { // set transaction heights for new main chain
                size_t count = BRMerkleBlockTxHashes(b, NULL, 0);
                uint32_t height = b->height, timestamp = b->timestamp;
                
                if (count > txCount) {
                    txHashes = (txHashes != _txHashes) ? realloc(txHashes, count*sizeof(*txHashes)) :
                               malloc(count*sizeof(*txHashes));
                    assert(txHashes != NULL);
                    txCount = count;
                }
                
                count = BRMerkleBlockTxHashes(b, txHashes, count);
                b = BRSetGet(manager->blocks, &b->prevBlock);
                if (b) timestamp = timestamp/2 + b->timestamp/2;
                if (count > 0) BRWalletUpdateTransactions(manager->wallet, txHashes, count, height, timestamp);
            }
        
            manager->lastBlock = block;
            
            if (block->height == manager->estimatedHeight) { // chain download is complete
                saveCount = (block->height % BLOCK_DIFFICULTY_INTERVAL) + BLOCK_DIFFICULTY_INTERVAL + 1;
                _BRPeerManagerLoadMempools(manager);
            }
        }
    }
   
    if (txHashes != _txHashes) free(txHashes);
   
    if (block && block->height != BLOCK_UNKNOWN_HEIGHT) {
        if (block->height > manager->estimatedHeight) manager->estimatedHeight = block->height;
        
        // check if the next block was received as an orphan
        orphan.prevBlock = block->blockHash;
        next = BRSetRemove(manager->orphans, &orphan);
    }
    
    BRMerkleBlock *saveBlocks[saveCount];
    
    for (i = 0, b = block; b && i < saveCount; i++) {
        saveBlocks[i] = b;
        b = BRSetGet(manager->blocks, &b->prevBlock);
    }
    
    // make sure the set of blocks to be saved starts at a difficulty interval
    j = (i > 0) ? saveBlocks[i - 1]->height % BLOCK_DIFFICULTY_INTERVAL : 0;
    if (j > 0) i -= (i > BLOCK_DIFFICULTY_INTERVAL - j) ? BLOCK_DIFFICULTY_INTERVAL - j : i;
    assert(i == 0 || (saveBlocks[i - 1]->height % BLOCK_DIFFICULTY_INTERVAL) == 0);
    pthread_mutex_unlock(&manager->lock);
    if (i > 0 && manager->saveBlocks) manager->saveBlocks(manager->info, (i > 1 ? 1 : 0), saveBlocks, i);
    
    if (block && block->height != BLOCK_UNKNOWN_HEIGHT && block->height >= BRPeerLastBlock(peer) &&
        manager->txStatusUpdate) {
        manager->txStatusUpdate(manager->info); // notify that transaction confirmations may have changed
    }
    
    if (next) _peerRelayedBlock(info, next);
}

static void _peerDataNotfound(void *info, const UInt256 txHashes[], size_t txCount,
                             const UInt256 blockHashes[], size_t blockCount)
{
    BRPeer *peer = ((BRPeerCallbackInfo *)info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *)info)->manager;

    pthread_mutex_lock(&manager->lock);

    for (size_t i = 0; i < txCount; i++) {
        _BRTxPeerListRemovePeer(manager->txRelays, txHashes[i], peer);
        _BRTxPeerListRemovePeer(manager->txRequests, txHashes[i], peer);
    }

    pthread_mutex_unlock(&manager->lock);
}

static void _peerSetFeePerKb(void *info, uint64_t feePerKb)
{
    BRPeer *p, *peer = ((BRPeerCallbackInfo *)info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *)info)->manager;
    uint64_t maxFeePerKb = 0, secondFeePerKb = 0;
    
    pthread_mutex_lock(&manager->lock);
    
    for (size_t i = array_count(manager->connectedPeers); i > 0; i--) { // find second highest fee rate
        p = manager->connectedPeers[i - 1];
        if (BRPeerConnectStatus(p) != BRPeerStatusConnected) continue;
        if (BRPeerFeePerKb(p) > maxFeePerKb) secondFeePerKb = maxFeePerKb, maxFeePerKb = BRPeerFeePerKb(p);
    }
    
    if (secondFeePerKb*3/2 > DEFAULT_FEE_PER_KB && secondFeePerKb*3/2 <= MAX_FEE_PER_KB &&
        secondFeePerKb*3/2 > BRWalletFeePerKb(manager->wallet)) {
        peer_log(peer, "increasing feePerKb to %llu based on feefilter messages from peers", secondFeePerKb*3/2);
        BRWalletSetFeePerKb(manager->wallet, secondFeePerKb*3/2);
    }

    pthread_mutex_unlock(&manager->lock);
}

//static void _peerRequestedTxPingDone(void *info, int success)
//{
//    BRPeer *peer = ((BRPeerCallbackInfo *)info)->peer;
//    BRPeerManager *manager = ((BRPeerCallbackInfo *)info)->manager;
//    UInt256 txHash = ((BRPeerCallbackInfo *)info)->hash;
//
//    free(info);
//    pthread_mutex_lock(&manager->lock);
//
//    if (success && ! _BRTxPeerListHasPeer(manager->txRequests, txHash, peer)) {
//        _BRTxPeerListAddPeer(&manager->txRequests, txHash, peer);
//        BRPeerSendGetdata(peer, &txHash, 1, NULL, 0); // check if peer will relay the transaction back
//    }
//    
//    pthread_mutex_unlock(&manager->lock);
//}

static BRTransaction *_peerRequestedTx(void *info, UInt256 txHash)
{
    BRPeer *peer = ((BRPeerCallbackInfo *)info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *)info)->manager;
//    BRPeerCallbackInfo *pingInfo;
    BRTransaction *tx = NULL;
    void *txInfo = NULL;
    void (*txCallback)(void *, int) = NULL;
    int hasPendingCallbacks = 0, error = 0;

    pthread_mutex_lock(&manager->lock);

    for (size_t i = array_count(manager->publishedTx); i > 0; i--) {
        if (UInt256Eq(manager->publishedTxHashes[i - 1], txHash)) {
            tx = manager->publishedTx[i - 1].tx;
            txInfo = manager->publishedTx[i - 1].info;
            txCallback = manager->publishedTx[i - 1].callback;
            manager->publishedTx[i - 1].info = NULL;
            manager->publishedTx[i - 1].callback = NULL;
        
            if (tx && ! BRWalletTransactionIsValid(manager->wallet, tx)) {
                error = EINVAL;
                array_rm(manager->publishedTx, i - 1);
                array_rm(manager->publishedTxHashes, i - 1);
                
                if (! BRWalletTransactionForHash(manager->wallet, txHash)) {
                    BRTransactionFree(tx);
                    tx = NULL;
                }
            }
        }
        else if (manager->publishedTx[i - 1].callback != NULL) hasPendingCallbacks = 1;
    }

    // cancel tx publish timeout if no publish callbacks are pending, and syncing is done or this is not downloadPeer
    if (! hasPendingCallbacks && (manager->syncStartHeight == 0 || peer != manager->downloadPeer)) {
        BRPeerScheduleDisconnect(peer, -1); // cancel publish tx timeout
    }

    if (tx && ! error) {
        _BRTxPeerListAddPeer(&manager->txRelays, txHash, peer);
        BRWalletRegisterTransaction(manager->wallet, tx);
    }
    
//    pingInfo = calloc(1, sizeof(*pingInfo));
//    assert(pingInfo != NULL);
//    pingInfo->peer = peer;
//    pingInfo->manager = manager;
//    pingInfo->hash = txHash;
//    BRPeerSendPing(peer, pingInfo, _peerRequestedTxPingDone);
    pthread_mutex_unlock(&manager->lock);
    if (txCallback) txCallback(txInfo, error);
    return tx;
}

static int _peerNetworkIsReachable(void *info)
{
    BRPeerManager *manager = ((BRPeerCallbackInfo *)info)->manager;

    return (manager->networkIsReachable) ? manager->networkIsReachable(manager->info) : 1;
}

static void _peerThreadCleanup(void *info)
{
    BRPeerManager *manager = ((BRPeerCallbackInfo *)info)->manager;

    free(info);
    if (manager->threadCleanup) manager->threadCleanup(manager->info);
}

static void _dummyThreadCleanup(void *info)
{
}

// returns a newly allocated BRPeerManager struct that must be freed by calling BRPeerManagerFree()
BRPeerManager *BRPeerManagerNew(BRWallet *wallet, uint32_t earliestKeyTime, BRMerkleBlock *blocks[], size_t blocksCount,
                                const BRPeer peers[], size_t peersCount)
{
    BRPeerManager *manager = calloc(1, sizeof(*manager));
    BRMerkleBlock orphan, *block = NULL;
    
    assert(manager != NULL);
    assert(wallet != NULL);
    assert(blocks != NULL || blocksCount == 0);
    assert(peers != NULL || peersCount == 0);
    manager->wallet = wallet;
    manager->earliestKeyTime = earliestKeyTime;
    manager->averageTxPerBlock = 1400;
    manager->maxConnectCount = PEER_MAX_CONNECTIONS;
    array_new(manager->peers, peersCount);
    if (peers) array_add_array(manager->peers, peers, peersCount);
    qsort(manager->peers, array_count(manager->peers), sizeof(*manager->peers), _peerTimestampCompare);
    array_new(manager->connectedPeers, PEER_MAX_CONNECTIONS);
    manager->blocks = BRSetNew(BRMerkleBlockHash, BRMerkleBlockEq, blocksCount);
    manager->orphans = BRSetNew(_BRPrevBlockHash, _BRPrevBlockEq, blocksCount); // orphans are indexed by prevBlock
    manager->checkpoints = BRSetNew(_BRBlockHeightHash, _BRBlockHeightEq, 100); // checkpoints are indexed by height

    for (size_t i = 0; i < CHECKPOINT_COUNT; i++) {
        block = BRMerkleBlockNew();
        block->height = checkpoint_array[i].height;
        block->blockHash = UInt256Reverse(u256_hex_decode(checkpoint_array[i].hash));
        block->timestamp = checkpoint_array[i].timestamp;
        block->target = checkpoint_array[i].target;
        BRSetAdd(manager->checkpoints, block);
        BRSetAdd(manager->blocks, block);
        if (i == 0 || block->timestamp + 7*24*60*60 < manager->earliestKeyTime) manager->lastBlock = block;
    }

    block = NULL;
    
    for (size_t i = 0; blocks && i < blocksCount; i++) {
        assert(blocks[i]->height != BLOCK_UNKNOWN_HEIGHT); // height must be saved/restored along with serialized block
        BRSetAdd(manager->orphans, blocks[i]);

        if ((blocks[i]->height % BLOCK_DIFFICULTY_INTERVAL) == 0 &&
            (! block || blocks[i]->height > block->height)) block = blocks[i]; // find last transition block
    }
    
    while (block) {
        BRSetAdd(manager->blocks, block);
        manager->lastBlock = block;
        orphan.prevBlock = block->prevBlock;
        BRSetRemove(manager->orphans, &orphan);
        orphan.prevBlock = block->blockHash;
        block = BRSetGet(manager->orphans, &orphan);
    }
    
    array_new(manager->txRelays, 10);
    array_new(manager->txRequests, 10);
    array_new(manager->publishedTx, 10);
    array_new(manager->publishedTxHashes, 10);
    pthread_mutex_init(&manager->lock, NULL);
    manager->threadCleanup = _dummyThreadCleanup;
    return manager;
}

// not thread-safe, set callbacks once before calling BRPeerManagerConnect()
// info is a void pointer that will be passed along with each callback call
// void syncStarted(void *) - called when blockchain syncing starts
// void syncStopped(void *, int) - called when blockchain syncing stops, error is an errno.h code
// void txStatusUpdate(void *) - called when transaction status may have changed such as when a new block arrives
// void saveBlocks(void *, int, BRMerkleBlock *[], size_t) - called when blocks should be saved to the persistent store
// - if replace is true, remove any previously saved blocks first
// void savePeers(void *, int, const BRPeer[], size_t) - called when peers should be saved to the persistent store
// - if replace is true, remove any previously saved peers first
// int networkIsReachable(void *) - must return true when networking is available, false otherwise
// void threadCleanup(void *) - called before a thread terminates to faciliate any needed cleanup
void BRPeerManagerSetCallbacks(BRPeerManager *manager, void *info,
                               void (*syncStarted)(void *info),
                               void (*syncStopped)(void *info, int error),
                               void (*txStatusUpdate)(void *info),
                               void (*saveBlocks)(void *info, int replace, BRMerkleBlock *blocks[], size_t blocksCount),
                               void (*savePeers)(void *info, int replace, const BRPeer peers[], size_t peersCount),
                               int (*networkIsReachable)(void *info),
                               void (*threadCleanup)(void *info))
{
    assert(manager != NULL);
    manager->info = info;
    manager->syncStarted = syncStarted;
    manager->syncStopped = syncStopped;
    manager->txStatusUpdate = txStatusUpdate;
    manager->saveBlocks = saveBlocks;
    manager->savePeers = savePeers;
    manager->networkIsReachable = networkIsReachable;
    manager->threadCleanup = (threadCleanup) ? threadCleanup : _dummyThreadCleanup;
}

// specifies a single fixed peer to use when connecting to the bitcoin network
// set address to UINT128_ZERO to revert to default behavior
void BRPeerManagerSetFixedPeer(BRPeerManager *manager, UInt128 address, uint16_t port)
{
    assert(manager != NULL);
    BRPeerManagerDisconnect(manager);
    pthread_mutex_lock(&manager->lock);
    manager->maxConnectCount = UInt128IsZero(address) ? PEER_MAX_CONNECTIONS : 1;
    manager->fixedPeer = ((BRPeer) { address, port, 0, 0, 0 });
    array_clear(manager->peers);
    pthread_mutex_unlock(&manager->lock);
}

// true if currently connected to at least one peer
int BRPeerManagerIsConnected(BRPeerManager *manager)
{
    int isConnected;
    
    assert(manager != NULL);
    pthread_mutex_lock(&manager->lock);
    isConnected = manager->isConnected;
    pthread_mutex_unlock(&manager->lock);
    return isConnected;
}

// connect to bitcoin peer-to-peer network (also call this whenever networkIsReachable() status changes)
void BRPeerManagerConnect(BRPeerManager *manager)
{
    assert(manager != NULL);
    pthread_mutex_lock(&manager->lock);
    if (manager->connectFailureCount >= MAX_CONNECT_FAILURES) manager->connectFailureCount = 0; //this is a manual retry
    
    if ((! manager->downloadPeer || manager->lastBlock->height < manager->estimatedHeight) &&
        manager->syncStartHeight == 0) {
        manager->syncStartHeight = manager->lastBlock->height + 1;
        pthread_mutex_unlock(&manager->lock);
        if (manager->syncStarted) manager->syncStarted(manager->info);
        pthread_mutex_lock(&manager->lock);
    }
    
    for (size_t i = array_count(manager->connectedPeers); i > 0; i--) {
        BRPeer *p = manager->connectedPeers[i - 1];

        if (BRPeerConnectStatus(p) == BRPeerStatusConnecting) BRPeerConnect(p);
    }
    
    if (array_count(manager->connectedPeers) < manager->maxConnectCount) {
        time_t now = time(NULL);
        BRPeer *peers;

        if (array_count(manager->peers) < manager->maxConnectCount ||
            manager->peers[manager->maxConnectCount - 1].timestamp + 3*24*60*60 < now) {
            _BRPeerManagerFindPeers(manager);
        }
        
        array_new(peers, 100);
        array_add_array(peers, manager->peers,
                        (array_count(manager->peers) < 100) ? array_count(manager->peers) : 100);

        while (array_count(peers) > 0 && array_count(manager->connectedPeers) < manager->maxConnectCount) {
            size_t i = BRRand((uint32_t)array_count(peers)); // index of random peer
            BRPeerCallbackInfo *info;
            
            i = i*i/array_count(peers); // bias random peer selection toward peers with more recent timestamp
        
            for (size_t j = array_count(manager->connectedPeers); i != SIZE_MAX && j > 0; j--) {
                if (! BRPeerEq(&peers[i], manager->connectedPeers[j - 1])) continue;
                array_rm(peers, i); // already in connectedPeers
                i = SIZE_MAX;
            }
            
            if (i != SIZE_MAX) {
                info = calloc(1, sizeof(*info));
                assert(info != NULL);
                info->manager = manager;
                info->peer = BRPeerNew();
                *info->peer = peers[i];
                array_rm(peers, i);
                array_add(manager->connectedPeers, info->peer);
                BRPeerSetCallbacks(info->peer, info, _peerConnected, _peerDisconnected, _peerRelayedPeers,
                                   _peerRelayedTx, _peerHasTx, _peerRejectedTx, _peerRelayedBlock, _peerDataNotfound,
                                   _peerSetFeePerKb, _peerRequestedTx, _peerNetworkIsReachable, _peerThreadCleanup);
                BRPeerSetEarliestKeyTime(info->peer, manager->earliestKeyTime);
                BRPeerConnect(info->peer);
            }
        }

        array_free(peers);
    }
    
    if (array_count(manager->connectedPeers) == 0) {
        peer_log(&BR_PEER_NONE, "sync failed");
        _BRPeerManagerSyncStopped(manager);
        pthread_mutex_unlock(&manager->lock);
        if (manager->syncStopped) manager->syncStopped(manager->info, ENETUNREACH);
    }
    else pthread_mutex_unlock(&manager->lock);
}

void BRPeerManagerDisconnect(BRPeerManager *manager)
{
    struct timespec ts;
    size_t peerCount, dnsThreadCount;
    
    assert(manager != NULL);
    pthread_mutex_lock(&manager->lock);
    peerCount = array_count(manager->connectedPeers);
    dnsThreadCount = manager->dnsThreadCount;
    
    for (size_t i = peerCount; i > 0; i--) {
        manager->connectFailureCount = MAX_CONNECT_FAILURES; // prevent futher automatic reconnect attempts
        BRPeerDisconnect(manager->connectedPeers[i - 1]);
    }
    
    pthread_mutex_unlock(&manager->lock);
    ts.tv_sec = 0;
    ts.tv_nsec = 1;
    
    while (peerCount > 0 || dnsThreadCount > 0) {
        nanosleep(&ts, NULL); // pthread_yield() isn't POSIX standard :(
        pthread_mutex_lock(&manager->lock);
        peerCount = array_count(manager->connectedPeers);
        dnsThreadCount = manager->dnsThreadCount;
        pthread_mutex_unlock(&manager->lock);
    }
}

// rescans blocks and transactions after earliestKeyTime (a new random download peer is also selected due to the
// possibility that a malicious node might lie by omitting transactions that match the bloom filter)
void BRPeerManagerRescan(BRPeerManager *manager)
{
    assert(manager != NULL);
    pthread_mutex_lock(&manager->lock);
    
    if (manager->isConnected) {
        // start the chain download from the most recent checkpoint that's at least a week older than earliestKeyTime
        for (size_t i = CHECKPOINT_COUNT; i > 0; i--) {
            if (i - 1 == 0 || checkpoint_array[i - 1].timestamp + 7*24*60*60 < manager->earliestKeyTime) {
                UInt256 hash = UInt256Reverse(u256_hex_decode(checkpoint_array[i - 1].hash));

                manager->lastBlock = BRSetGet(manager->blocks, &hash);
                break;
            }
        }
        
        if (manager->downloadPeer) { // disconnect the current download peer so a new random one will be selected
            for (size_t i = array_count(manager->peers); i > 0; i--) {
                if (BRPeerEq(&manager->peers[i - 1], manager->downloadPeer)) array_rm(manager->peers, i - 1);
            }
            
            BRPeerDisconnect(manager->downloadPeer);
        }

        manager->syncStartHeight = 0; // a syncStartHeight of 0 indicates that syncing hasn't started yet
        pthread_mutex_unlock(&manager->lock);
        BRPeerManagerConnect(manager);
    }
    else pthread_mutex_unlock(&manager->lock);
}

// the (unverified) best block height reported by connected peers
uint32_t BRPeerManagerEstimatedBlockHeight(BRPeerManager *manager)
{
    uint32_t height;
    
    assert(manager != NULL);
    pthread_mutex_lock(&manager->lock);
    height = (manager->lastBlock->height < manager->estimatedHeight) ? manager->estimatedHeight :
             manager->lastBlock->height;
    pthread_mutex_unlock(&manager->lock);
    return height;
}

// current proof-of-work verified best block height
uint32_t BRPeerManagerLastBlockHeight(BRPeerManager *manager)
{
    uint32_t height;
    
    assert(manager != NULL);
    pthread_mutex_lock(&manager->lock);
    height = manager->lastBlock->height;
    pthread_mutex_unlock(&manager->lock);
    return height;
}

// current proof-of-work verified best block timestamp (time interval since unix epoch)
uint32_t BRPeerManagerLastBlockTimestamp(BRPeerManager *manager)
{
    uint32_t timestamp;
    
    assert(manager != NULL);
    pthread_mutex_lock(&manager->lock);
    timestamp = manager->lastBlock->timestamp;
    pthread_mutex_unlock(&manager->lock);
    return timestamp;
}

// current network sync progress from 0 to 1
// startHeight is the block height of the most recent fully completed sync
double BRPeerManagerSyncProgress(BRPeerManager *manager, uint32_t startHeight)
{
    double progress;
    
    assert(manager != NULL);
    pthread_mutex_lock(&manager->lock);
    if (startHeight == 0) startHeight = manager->syncStartHeight;
    
    if (! manager->downloadPeer && manager->syncStartHeight == 0) {
        progress = 0.0;
    }
    else if (! manager->downloadPeer || manager->lastBlock->height < manager->estimatedHeight) {
        if (manager->lastBlock->height > startHeight && manager->estimatedHeight > startHeight) {
            progress = 0.1 + 0.9*(manager->lastBlock->height - startHeight)/(manager->estimatedHeight - startHeight);
        }
        else progress = 0.05;
    }
    else progress = 1.0;

    pthread_mutex_unlock(&manager->lock);
    return progress;
}

// returns the number of currently connected peers
size_t BRPeerManagerPeerCount(BRPeerManager *manager)
{
    size_t count = 0;
    
    assert(manager != NULL);
    pthread_mutex_lock(&manager->lock);
    
    for (size_t i = array_count(manager->connectedPeers); i > 0; i--) {
        if (BRPeerConnectStatus(manager->connectedPeers[i - 1]) == BRPeerStatusConnected) count++;
    }
    
    pthread_mutex_unlock(&manager->lock);
    return count;
}

// description of the peer most recently used to sync blockchain data
const char *BRPeerManagerDownloadPeerName(BRPeerManager *manager)
{
    assert(manager != NULL);
    pthread_mutex_lock(&manager->lock);

    if (manager->downloadPeer) {
        sprintf(manager->downloadPeerName, "%s:%d", BRPeerHost(manager->downloadPeer), manager->downloadPeer->port);
    }
    else manager->downloadPeerName[0] = '\0';
    
    pthread_mutex_unlock(&manager->lock);
    return manager->downloadPeerName;
}

static void _publishTxInvDone(void *info, int success)
{
    BRPeer *peer = ((BRPeerCallbackInfo *)info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *)info)->manager;
    
    free(info);
    pthread_mutex_lock(&manager->lock);
    _BRPeerManagerRequestUnrelayedTx(manager, peer);
    pthread_mutex_unlock(&manager->lock);
}

// publishes tx to bitcoin network (do not call BRTransactionFree() on tx afterward)
void BRPeerManagerPublishTx(BRPeerManager *manager, BRTransaction *tx, void *info,
                            void (*callback)(void *info, int error))
{
    assert(manager != NULL);
    assert(tx != NULL && BRTransactionIsSigned(tx));
    if (tx) pthread_mutex_lock(&manager->lock);
    
    if (tx && ! BRTransactionIsSigned(tx)) {
        pthread_mutex_unlock(&manager->lock);
        BRTransactionFree(tx);
        tx = NULL;
        if (callback) callback(info, EINVAL); // transaction not signed
    }
    else if (tx && ! manager->isConnected) {
        int connectFailureCount = manager->connectFailureCount;

        pthread_mutex_unlock(&manager->lock);

        if (connectFailureCount >= MAX_CONNECT_FAILURES ||
            (manager->networkIsReachable && ! manager->networkIsReachable(manager->info))) {
            BRTransactionFree(tx);
            tx = NULL;
            if (callback) callback(info, ENOTCONN); // not connected to bitcoin network
        }
        else pthread_mutex_lock(&manager->lock);
    }
    
    if (tx) {
        size_t i, count = 0;
        
        tx->timestamp = (uint32_t)time(NULL); // set timestamp to publish time
        _BRPeerManagerAddTxToPublishList(manager, tx, info, callback);

        for (i = array_count(manager->connectedPeers); i > 0; i--) {
            if (BRPeerConnectStatus(manager->connectedPeers[i - 1]) == BRPeerStatusConnected) count++;
        }

        for (i = array_count(manager->connectedPeers); i > 0; i--) {
            BRPeer *peer = manager->connectedPeers[i - 1];
            BRPeerCallbackInfo *peerInfo;

            if (BRPeerConnectStatus(peer) != BRPeerStatusConnected) continue;
            
            // instead of publishing to all peers, leave out downloadPeer to see if tx propogates/gets relayed back
            // TODO: XXX connect to a random peer with an empty or fake bloom filter just for publishing
            if (peer != manager->downloadPeer || count == 1) {
                _BRPeerManagerPublishPendingTx(manager, peer);
                peerInfo = calloc(1, sizeof(*peerInfo));
                assert(peerInfo != NULL);
                peerInfo->peer = peer;
                peerInfo->manager = manager;
                BRPeerSendPing(peer, peerInfo, _publishTxInvDone);
            }
        }

        pthread_mutex_unlock(&manager->lock);
    }
}

// number of connected peers that have relayed the given unconfirmed transaction
size_t BRPeerManagerRelayCount(BRPeerManager *manager, UInt256 txHash)
{
    size_t count = 0;

    assert(manager != NULL);
    assert(! UInt256IsZero(txHash));
    pthread_mutex_lock(&manager->lock);
    
    for (size_t i = array_count(manager->txRelays); i > 0; i--) {
        if (! UInt256Eq(manager->txRelays[i - 1].txHash, txHash)) continue;
        count = array_count(manager->txRelays[i - 1].peers);
        break;
    }
    
    pthread_mutex_unlock(&manager->lock);
    return count;
}

// frees memory allocated for manager
void BRPeerManagerFree(BRPeerManager *manager)
{
    assert(manager != NULL);
    pthread_mutex_lock(&manager->lock);
    array_free(manager->peers);
    for (size_t i = array_count(manager->connectedPeers); i > 0; i--) BRPeerFree(manager->connectedPeers[i - 1]);
    array_free(manager->connectedPeers);
    BRSetApply(manager->blocks, NULL, _setApplyFreeBlock);
    BRSetFree(manager->blocks);
    BRSetApply(manager->orphans, NULL, _setApplyFreeBlock);
    BRSetFree(manager->orphans);
    BRSetFree(manager->checkpoints);
    for (size_t i = array_count(manager->txRelays); i > 0; i--) free(manager->txRelays[i - 1].peers);
    array_free(manager->txRelays);
    for (size_t i = array_count(manager->txRequests); i > 0; i--) free(manager->txRequests[i - 1].peers);
    array_free(manager->txRequests);
    array_free(manager->publishedTx);
    array_free(manager->publishedTxHashes);
    pthread_mutex_unlock(&manager->lock);
    pthread_mutex_destroy(&manager->lock);
    free(manager);
}
