//
//  BRChainParams.h
//
//  Created by Aaron Voisine on 1/10/18.
//  Copyright (c) 2019 breadwallet LLC
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

#ifndef BRChainParams_h
#define BRChainParams_h

#include "BRMerkleBlock.h"
#include "BRSet.h"
#include <assert.h>

typedef struct {
    uint32_t height;
    UInt256 hash;
    uint32_t timestamp;
    uint32_t target;
} BRCheckPoint;

typedef struct {
    const char * const *dnsSeeds; // NULL terminated array of dns seeds
    uint16_t standardPort;
    uint32_t magicNumber;
    uint64_t services;
    int (*verifyDifficulty)(const BRMerkleBlock *block, const BRSet *blockSet); // blockSet must have last 2016 blocks
    const BRCheckPoint *checkpoints;
    size_t checkpointsCount;
} BRChainParams;

static const char *BRMainNetDNSSeeds[] = {
    "dnsseed.monacoin.org.", "monacoin.seed.lapool.me.", "dnsseed.tamami-foundation.org.", NULL
};

static const char *BRTestNetDNSSeeds[] = {
    "testnet-dnsseed.monacoin.org.", NULL
};

// blockchain checkpoints - these are also used as starting points for partial chain downloads, so they must be at
// difficulty transition boundaries in order to verify the block difficulty at the immediately following transition
static const BRCheckPoint BRMainNetCheckpoints[] = {
    {       0, uint256("ff9f1c0116d19de7c9963845e129f9ed1bfc0b376eb54fd7afa42e0d418c8bb6"), 1388479472, 0x1e0ffff0 },
    {  100800, uint256("3e3cfc1e6573fe128865c9273bb1126120096afe706da98798be2decc56c276f"), 1397003984, 0x1c0da7a6 },
    {  201600, uint256("061b8b99b37e5234a4d32775d74c3a45703e34b89e796eafbcd6ca0804426e7c"), 1406373989, 0x1c0125c5 },
    {  302400, uint256("ffff3d65d170d8a515d77a0439dee9f6a969097f82b6061f8e1a2cd9a9ef6024"), 1418879501, 0x1c008127 },
    {  403200, uint256("67c31a52f881d020dcaf3ab14e027f4d0710a16263502611d8688d8b86ae9e2a"), 1433578614, 0x1b26ac5d },
    {  504000, uint256("e3d813e519a79b8bc4d7b35bb923c9992dc4c0bcca8d07d23ae9c10660d52408"), 1448443781, 0x1b5e2d4d },
    {  604800, uint256("02d0510ca18d081b67df44a3f4798c55597a25ce9bba442de825068e0cbd81ad"), 1457956652, 0x1c00f8f3 },
    {  705600, uint256("937ea6d2256dfbe8f3495fe545cce9964be566967a33055ee66bf8b8e1129d46"), 1467463781, 0x1b6c29a1 },
    {  806400, uint256("4987de18a5ee0896c252a09521fd568312046b142c5b36ed73b6000685ca3479"), 1476975211, 0x1b622a9c },
    {  907200, uint256("cae1cc406fd228fe57e44b157c5f204772002b0c67f99cd5fcc18452fe748bac"), 1486526968, 0x1c00864f },
    { 1008000, uint256("0941421e644b7a1a872b2809a1ae43e8da3dbf4d925fae34caa9cacd2859c614"), 1496138162, 0x1b18edc8 },
    { 1108800, uint256("5be09a3965912fbe9c56d41140df592e92bc6b72b57fd96dda163623ac83a9e7"), 1505659451, 0x1b07dbad },
    { 1209600, uint256("3946bcc31371f8fc5227ae29cd74b0e80acb73973ab70130041c0212d55d6a8c"), 1515211040, 0x1b01cb85 },
    { 1310400, uint256("572cf2547472b919a5b46f2783603174df14a8e7b0e7645bf8aa43b7a2eed41d"), 1524747242, 0x1b01dd10 },
    { 1411200, uint256("5d6ee34fbe619e459f6fd6639adeafc618b4c18db4026eaa9f248bac8ec5abf2"), 1534274188, 0x1b0148ca },
    { 1512000, uint256("0a44aff821099e59cbce8cbc08ac0b79835a9f23ad57f710d5be2cb0754446ff"), 1543783044, 0x1a3357ce },
    { 1612800, uint256("e2aabd02174ac273baf430a387930f34a2487a6df80a15177c7b55a258ef4097"), 1553294574, 0x1a302593 }
};

static const BRCheckPoint BRTestNetCheckpoints[] = {
    {       0, uint256("a2b106ceba3be0c6d097b2a6a6aacf9d638ba8258ae478158f449c321061e0b2"), 1488924140, 0x1e0ffff0 },
    {  100800, uint256("5ac6b3023321d67d8cd19379bdc0c7a492a0c9f8028d7f976c9c6685d3e0e148"), 1512001767, 0x1e010cbd },
    {  201600, uint256("ca072a2cb4f53d3de90482cb24787a535bef784d0e23b0e7de86b31a6e18075c"), 1522245582, 0x1e01067f }
};

static int BRMainNetVerifyDifficulty(const BRMerkleBlock *block, const BRSet *blockSet)
{
    // const BRMerkleBlock *previous, *b = NULL;
    // uint32_t i;

    // assert(block != NULL);
    // assert(blockSet != NULL);

    // // check if we hit a difficulty transition, and find previous transition block
    // if ((block->height % BLOCK_DIFFICULTY_INTERVAL) == 0) {
    //     for (i = 0, b = block; b && i < BLOCK_DIFFICULTY_INTERVAL; i++) {
    //         b = BRSetGet(blockSet, &b->prevBlock);
    //     }
    // }

    // previous = BRSetGet(blockSet, &block->prevBlock);
    // return BRMerkleBlockVerifyDifficulty(block, previous, (b) ? b->timestamp : 0);
    return 1;
}

static int BRTestNetVerifyDifficulty(const BRMerkleBlock *block, const BRSet *blockSet)
{
    return 1; // XXX skip testnet difficulty check for now
}

static const BRChainParams BRMainNetParams = {
    BRMainNetDNSSeeds,
    9401,       // standardPort
    0xdbb6c0fb, // magicNumber
    0,          // services
    BRMainNetVerifyDifficulty,
    BRMainNetCheckpoints,
    sizeof(BRMainNetCheckpoints) / sizeof(*BRMainNetCheckpoints)};

static const BRChainParams BRTestNetParams = {
    BRTestNetDNSSeeds,
    19403,      // standardPort
    0xf1c8d2fd, // magicNumber
    0,          // services
    BRTestNetVerifyDifficulty,
    BRTestNetCheckpoints,
    sizeof(BRTestNetCheckpoints) / sizeof(*BRTestNetCheckpoints)};

#endif // BRChainParams_h
