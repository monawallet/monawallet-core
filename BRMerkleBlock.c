//
//  BRMerkleBlock.c
//
//  Created by Aaron Voisine on 8/6/15.
//  Copyright (c) 2015 breadwallet LLC
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

#include "BRMerkleBlock.h"
#include "BRCrypto.h"
#include "BRAddress.h"
#include <stdlib.h>
#include <inttypes.h>
#include <limits.h>
#include <string.h>
#include <assert.h>

#define MAX_PROOF_OF_WORK 0x1e0fffff    // highest value for difficulty target (higher values are less difficult)
#define TARGET_SPACING    (1.5 * 60)     // 1.5 minutes

#if BITCOIN_TESTNET
#define SWITCH_LYRA_DGW_BLOCK   30
#else
#define SWITCH_LYRA_DGW_BLOCK   450000
#endif

inline static int _ceil_log2(int x)
{
    int r = (x & (x - 1)) ? 1 : 0;
    
    while ((x >>= 1) != 0) r++;
    return r;
}

// from https://en.bitcoin.it/wiki/Protocol_specification#Merkle_Trees
// Merkle trees are binary trees of hashes. Merkle trees in bitcoin use a double SHA-256, the SHA-256 hash of the
// SHA-256 hash of something. If, when forming a row in the tree (other than the root of the tree), it would have an odd
// number of elements, the final double-hash is duplicated to ensure that the row has an even number of hashes. First
// form the bottom row of the tree with the ordered double-SHA-256 hashes of the byte streams of the transactions in the
// block. Then the row above it consists of half that number of hashes. Each entry is the double-SHA-256 of the 64-byte
// concatenation of the corresponding two hashes below it in the tree. This procedure repeats recursively until we reach
// a row consisting of just a single double-hash. This is the merkle root of the tree.
//
// from https://github.com/bitcoin/bips/blob/master/bip-0037.mediawiki#Partial_Merkle_branch_format
// The encoding works as follows: we traverse the tree in depth-first order, storing a bit for each traversed node,
// signifying whether the node is the parent of at least one matched leaf txid (or a matched txid itself). In case we
// are at the leaf level, or this bit is 0, its merkle node hash is stored, and its children are not explored further.
// Otherwise, no hash is stored, but we recurse into both (or the only) child branch. During decoding, the same
// depth-first traversal is performed, consuming bits and hashes as they were written during encoding.
//
// example tree with three transactions, where only tx2 is matched by the bloom filter:
//
//     merkleRoot
//      /     \
//    m1       m2
//   /  \     /  \
// tx1  tx2 tx3  tx3
//
// flag bits (little endian): 00001011 [merkleRoot = 1, m1 = 1, tx1 = 0, tx2 = 1, m2 = 0, byte padding = 000]
// hashes: [tx1, tx2, m2]
//
// NOTE: this merkle tree design has a security vulnerability (CVE-2012-2459), which can be defended against by
// considering the merkle root invalid if there are duplicate hashes in any rows with an even number of elements

// returns a newly allocated merkle block struct that must be freed by calling BRMerkleBlockFree()
BRMerkleBlock *BRMerkleBlockNew(void)
{
    BRMerkleBlock *block = calloc(1, sizeof(*block));

    assert(block != NULL);
    
    block->height = BLOCK_UNKNOWN_HEIGHT;
    return block;
}

// buf must contain either a serialized merkleblock or header
// returns a merkle block struct that must be freed by calling BRMerkleBlockFree()
BRMerkleBlock *BRMerkleBlockParse(const uint8_t *buf, size_t bufLen, uint32_t currentHeight)
{
    BRMerkleBlock *block = (buf && 80 <= bufLen) ? BRMerkleBlockNew() : NULL;
    size_t off = 0, len = 0;
    
    assert(buf != NULL || bufLen == 0);
    
    if (block) {
        block->version = UInt32GetLE(&buf[off]);
        off += sizeof(uint32_t);
        block->prevBlock = UInt256Get(&buf[off]);
        off += sizeof(UInt256);
        block->merkleRoot = UInt256Get(&buf[off]);
        off += sizeof(UInt256);
        block->timestamp = UInt32GetLE(&buf[off]);
        off += sizeof(uint32_t);
        block->target = UInt32GetLE(&buf[off]);
        off += sizeof(uint32_t);
        block->nonce = UInt32GetLE(&buf[off]);
        off += sizeof(uint32_t);
        
        block->height = currentHeight;
        
        if (off + sizeof(uint32_t) <= bufLen) {
            block->totalTx = UInt32GetLE(&buf[off]);
            off += sizeof(uint32_t);
            block->hashesCount = (size_t)BRVarInt(&buf[off], (off <= bufLen ? bufLen - off : 0), &len);
            off += len;
            len = block->hashesCount*sizeof(UInt256);
            block->hashes = (off + len <= bufLen) ? malloc(len) : NULL;
            if (block->hashes) memcpy(block->hashes, &buf[off], len);
            off += len;
            block->flagsLen = (size_t)BRVarInt(&buf[off], (off <= bufLen ? bufLen - off : 0), &len);
            off += len;
            len = block->flagsLen;
            block->flags = (off + len <= bufLen) ? malloc(len) : NULL;
            if (block->flags) memcpy(block->flags, &buf[off], len);
        }
        if (block->height < SWITCH_LYRA_DGW_BLOCK - 1) {
            BRScrypt(&block->powHash, sizeof(block->powHash), buf, 80, buf, 80, 1024, 1, 1);
        }else{
            BRLyra2REv2((const char*)buf, (char*)&block->powHash);
        }
        BRSHA256_2(&block->blockHash, buf, 80);
    }
    
    return block;
}

// returns number of bytes written to buf, or total bufLen needed if buf is NULL (block->height is not serialized)
size_t BRMerkleBlockSerialize(const BRMerkleBlock *block, uint8_t *buf, size_t bufLen)
{
    size_t off = 0, len = 80;
    
    assert(block != NULL);
    
    if (block->totalTx > 0) {
        len += sizeof(uint32_t) + BRVarIntSize(block->hashesCount) + block->hashesCount*sizeof(UInt256) +
               BRVarIntSize(block->flagsLen) + block->flagsLen;
    }
    
    if (buf && len <= bufLen) {
        UInt32SetLE(&buf[off], block->version);
        off += sizeof(uint32_t);
        UInt256Set(&buf[off], block->prevBlock);
        off += sizeof(UInt256);
        UInt256Set(&buf[off], block->merkleRoot);
        off += sizeof(UInt256);
        UInt32SetLE(&buf[off], block->timestamp);
        off += sizeof(uint32_t);
        UInt32SetLE(&buf[off], block->target);
        off += sizeof(uint32_t);
        UInt32SetLE(&buf[off], block->nonce);
        off += sizeof(uint32_t);
    
        if (block->totalTx > 0) {
            UInt32SetLE(&buf[off], block->totalTx);
            off += sizeof(uint32_t);
            off += BRVarIntSet(&buf[off], (off <= bufLen ? bufLen - off : 0), block->hashesCount);
            if (block->hashes) memcpy(&buf[off], block->hashes, block->hashesCount*sizeof(UInt256));
            off += block->hashesCount*sizeof(UInt256);
            off += BRVarIntSet(&buf[off], (off <= bufLen ? bufLen - off : 0), block->flagsLen);
            if (block->flags) memcpy(&buf[off], block->flags, block->flagsLen);
            off += block->flagsLen;
        }
    }
    
    return (! buf || len <= bufLen) ? len : 0;
}

static size_t _BRMerkleBlockTxHashesR(const BRMerkleBlock *block, UInt256 *txHashes, size_t hashesCount, size_t *idx,
                                      size_t *hashIdx, size_t *flagIdx, int depth)
{
    uint8_t flag;
    
    if (*flagIdx/8 < block->flagsLen && *hashIdx < block->hashesCount) {
        flag = (block->flags[*flagIdx/8] & (1 << (*flagIdx % 8)));
        (*flagIdx)++;
    
        if (! flag || depth == _ceil_log2(block->totalTx)) {
            if (flag && *idx < hashesCount) {
                if (txHashes) txHashes[*idx] = block->hashes[*hashIdx]; // leaf
                (*idx)++;
            }
        
            (*hashIdx)++;
        }
        else {
            _BRMerkleBlockTxHashesR(block, txHashes, hashesCount, idx, hashIdx, flagIdx, depth + 1); // left branch
            _BRMerkleBlockTxHashesR(block, txHashes, hashesCount, idx, hashIdx, flagIdx, depth + 1); // right branch
        }
    }

    return *idx;
}

// populates txHashes with the matched tx hashes in the block
// returns number of hashes written, or the total hashesCount needed if txHashes is NULL
size_t BRMerkleBlockTxHashes(const BRMerkleBlock *block, UInt256 *txHashes, size_t hashesCount)
{
    size_t idx = 0, hashIdx = 0, flagIdx = 0;

    assert(block != NULL);
    
    return _BRMerkleBlockTxHashesR(block, txHashes, (txHashes) ? hashesCount : SIZE_MAX, &idx, &hashIdx, &flagIdx, 0);
}

// sets the hashes and flags fields for a block created with BRMerkleBlockNew()
void BRMerkleBlockSetTxHashes(BRMerkleBlock *block, const UInt256 hashes[], size_t hashesCount,
                              const uint8_t *flags, size_t flagsLen)
{
    assert(block != NULL);
    assert(hashes != NULL || hashesCount == 0);
    assert(flags != NULL || flagsLen == 0);
    
    // TODO: fix difficulty target check for Monacoin
    if (block->hashes) free(block->hashes);
    block->hashes = (hashesCount > 0) ? malloc(hashesCount*sizeof(UInt256)) : NULL;
    if (block->hashes) memcpy(block->hashes, hashes, hashesCount*sizeof(UInt256));
    if (block->flags) free(block->flags);
    block->flags = (flagsLen > 0) ? malloc(flagsLen) : NULL;
    if (block->flags) memcpy(block->flags, flags, flagsLen);
}

// recursively walks the merkle tree to calculate the merkle root
// NOTE: this merkle tree design has a security vulnerability (CVE-2012-2459), which can be defended against by
// considering the merkle root invalid if there are duplicate hashes in any rows with an even number of elements
static UInt256 _BRMerkleBlockRootR(const BRMerkleBlock *block, size_t *hashIdx, size_t *flagIdx, int depth)
{
    uint8_t flag;
    UInt256 hashes[2], md = UINT256_ZERO;

    if (*flagIdx/8 < block->flagsLen && *hashIdx < block->hashesCount) {
        flag = (block->flags[*flagIdx/8] & (1 << (*flagIdx % 8)));
        (*flagIdx)++;

        if (flag && depth != _ceil_log2(block->totalTx)) {
            hashes[0] = _BRMerkleBlockRootR(block, hashIdx, flagIdx, depth + 1); // left branch
            hashes[1] = _BRMerkleBlockRootR(block, hashIdx, flagIdx, depth + 1); // right branch

            if (! UInt256IsZero(hashes[0]) && ! UInt256Eq(hashes[0], hashes[1])) {
                if (UInt256IsZero(hashes[1])) hashes[1] = hashes[0]; // if right branch is missing, dup left branch
                BRSHA256_2(&md, hashes, sizeof(hashes));
            }
            else *hashIdx = SIZE_MAX; // defend against (CVE-2012-2459)
        }
        else md = block->hashes[(*hashIdx)++]; // leaf
    }
    
    return md;
}

// true if merkle tree and timestamp are valid, and proof-of-work matches the stated difficulty target
// NOTE: this only checks if the block difficulty matches the difficulty target in the header, it does not check if the
// target is correct for the block's height in the chain - use BRMerkleBlockVerifyDifficulty() for that
int BRMerkleBlockIsValid(const BRMerkleBlock *block, uint32_t currentTime)
{
    assert(block != NULL);
    
    // target is in "compact" format, where the most significant byte is the size of resulting value in bytes, the next
    // bit is the sign, and the remaining 23bits is the value after having been right shifted by (size - 3)*8 bits
    static const uint32_t maxsize = MAX_PROOF_OF_WORK >> 24, maxtarget = MAX_PROOF_OF_WORK & 0x00ffffff;
    const uint32_t size = block->target >> 24, target = block->target & 0x00ffffff;
    size_t hashIdx = 0, flagIdx = 0;
    UInt256 merkleRoot = _BRMerkleBlockRootR(block, &hashIdx, &flagIdx, 0), t = UINT256_ZERO;
    int r = 1;
    
    // check if merkle root is correct
    if (block->totalTx > 0 && ! UInt256Eq(merkleRoot, block->merkleRoot)) r = 0;
    
    // check if timestamp is too far in future
    if (block->timestamp > currentTime + BLOCK_MAX_TIME_DRIFT) r = 0;
    
    // check if proof-of-work target is out of range
    if (target == 0 || target & 0x00800000 || size > maxsize || (size == maxsize && target > maxtarget)) r = 0;
    
    if (size > 3) UInt32SetLE(&t.u8[size - 3], target);
    else UInt32SetLE(t.u8, target >> (3 - size)*8);
    
    for (int i = sizeof(t) - 1; r && i >= 0; i--) { // check proof-of-work
        if (block->powHash.u8[i] < t.u8[i]) break;
        if (block->powHash.u8[i] > t.u8[i]) r = 0;
    }
    return r;
}

// true if the given tx hash is known to be included in the block
int BRMerkleBlockContainsTxHash(const BRMerkleBlock *block, UInt256 txHash)
{
    int r = 0;
    
    assert(block != NULL);
    assert(! UInt256IsZero(txHash));
    
    for (size_t i = 0; ! r && i < block->hashesCount; i++) {
        if (UInt256Eq(block->hashes[i], txHash)) r = 1;
    }
    
    return r;
}

// verifies the block difficulty target is correct for the block's position in the chain
// transitionTime is the timestamp of the block at the previous difficulty transition
// transitionTime may be 0 if block->height is not a multiple of BLOCK_DIFFICULTY_INTERVAL
//
// The difficulty target algorithm works as follows:
// The target must be the same as in the previous block unless the block's height is a multiple of 2016. Every 2016
// blocks there is a difficulty transition where a new difficulty is calculated. The new target is the previous target
// multiplied by the time between the last transition block's timestamp and this one (in seconds), divided by the
// targeted time between transitions (14*24*60*60 seconds). If the new difficulty is more than 4x or less than 1/4 of
// the previous difficulty, the change is limited to either 4x or 1/4. There is also a minimum difficulty value
// intuitively named MAX_PROOF_OF_WORK... since larger values are less difficult.
int BRMerkleBlockVerifyDifficulty(const BRMerkleBlock *block, const BRMerkleBlock *previous, const BRSet *blocks)
{
    int r = 1;
    
    assert(block != NULL);
    assert(previous != NULL);
    assert(blocks != NULL);
    
    if (! previous || !UInt256Eq(block->prevBlock, previous->blockHash) || block->height != previous->height + 1) r = 0;

    if (block->height >= SWITCH_LYRA_DGW_BLOCK) {

        int32_t darkGravityWaveTarget = darkGravityWaveTargetWithPreviousBlocks(blocks, previous);
        int32_t diff = block->target - darkGravityWaveTarget;

        {
            static uint pastCount = 0;
            if (pastCount < DGW_PAST_BLOCKS_MAX ) {
#if BITCOIN_TESTNET
                r = 1;
#else
                if (block->height == 1350721) r = block->target == 0x1b01ccec ? 1 : 0;
                if (block->height == 1350722) r = block->target == 0x1b01daff ? 1 : 0;
                if (block->height == 1350723) r = block->target == 0x1b01e500 ? 1 : 0;
                if (block->height == 1350724) r = block->target == 0x1b01dc98 ? 1 : 0;
                if (block->height == 1350725) r = block->target == 0x1b019aca ? 1 : 0;
                if (block->height == 1350726) r = block->target == 0x1b01b265 ? 1 : 0;
                if (block->height == 1350727) r = block->target == 0x1b01bed3 ? 1 : 0;
                if (block->height == 1350728) r = block->target == 0x1b01b6db ? 1 : 0;
                if (block->height == 1350729) r = block->target == 0x1b01d499 ? 1 : 0;
                if (block->height == 1350730) r = block->target == 0x1b01ce17 ? 1 : 0;
                if (block->height == 1350731) r = block->target == 0x1b01cdd9 ? 1 : 0;
                if (block->height == 1350732) r = block->target == 0x1b01b153 ? 1 : 0;
                if (block->height == 1350733) r = block->target == 0x1b01b87b ? 1 : 0;
                if (block->height == 1350734) r = block->target == 0x1b019c70 ? 1 : 0;
                if (block->height == 1350735) r = block->target == 0x1b01986e ? 1 : 0;
                if (block->height == 1350736) r = block->target == 0x1b018982 ? 1 : 0;
                if (block->height == 1350737) r = block->target == 0x1b0176d4 ? 1 : 0;
                if (block->height == 1350738) r = block->target == 0x1b014d66 ? 1 : 0;
                if (block->height == 1350739) r = block->target == 0x1b013e29 ? 1 : 0;
                if (block->height == 1350740) r = block->target == 0x1b013431 ? 1 : 0;
                if (block->height == 1350741) r = block->target == 0x1b013a27 ? 1 : 0;
                if (block->height == 1350742) r = block->target == 0x1b015b09 ? 1 : 0;
                if (block->height == 1350743) r = block->target == 0x1b016d30 ? 1 : 0;
                if (block->height == 1350744) r = block->target == 0x1b016e21 ? 1 : 0;
#endif
            } else {
                r = (abs(diff) < 2) ? 1 : 0; //the core client is less precise with a rounding error that can sometimes cause a problem. We are very rarely 1 off
            }
            pastCount ++;
        }
    }
    return r;
}


int32_t darkGravityWaveTargetWithPreviousBlocks(const BRSet *blocks, const BRMerkleBlock *previous)
{
    /* current difficulty formula, darkcoin - based on DarkGravity v3, original work done by evan duffield, modified for iOS */
    BRMerkleBlock *previousBlock = BRSetGet(blocks, previous);
    
    int32_t nActualTimespan = 0;
    int64_t lastBlockTime = 0;
    int64_t blockCount = 0;
    UInt256 sumTargets = UINT256_ZERO;
    
    if (!previousBlock || previousBlock->height == 0 || previousBlock->height < SWITCH_LYRA_DGW_BLOCK + DGW_PAST_BLOCKS_MIN) {
        // This is the first block or the height is < PastBlocksMin
        // Return minimal required work. (1e0ffff)
        return MAX_PROOF_OF_WORK;
    }
    
    //BRMerkleBlock *currentBlock = previousBlock;
    BRMerkleBlock *currentBlock = previousBlock;
    // loop over the past n blocks, where n == PastBlocksMax
    for (blockCount = 1; currentBlock && currentBlock->height  > 0 && blockCount<=DGW_PAST_BLOCKS_MAX; blockCount++) {
        
        // Calculate average difficulty based on the blocks we iterate over in this for loop
        if(blockCount <= DGW_PAST_BLOCKS_MIN) {
            UInt256 currentTarget = setCompact(currentBlock->target);
            
            if (blockCount == 1) {
                sumTargets = add(currentTarget,currentTarget);
            } else {
                sumTargets = add(sumTargets,currentTarget);
            }
        }
        
        // If this is the second iteration (LastBlockTime was set)
        if(lastBlockTime > 0){
            // Calculate time difference between previous block and current block
            int64_t currentBlockTime = currentBlock->timestamp;
            int64_t diff = ((lastBlockTime) - (currentBlockTime));
            // Increment the actual timespan
            nActualTimespan += diff;
        }
        // Set lastBlockTime to the block time for the block in current iteration
        lastBlockTime = currentBlock->timestamp;
        
        if (previousBlock == NULL) { assert(currentBlock); break; }
        currentBlock = BRSetGet(blocks, &currentBlock->prevBlock);
    }
    
    UInt256 blockCount256 = ((UInt256) { .u64 = { blockCount, 0, 0, 0 } });
    // darkTarget is the difficulty
    UInt256 darkTarget = divide(sumTargets,blockCount256);
    
    // nTargetTimespan is the time that the CountBlocks should have taken to be generated.
    uint32_t nTargetTimespan = (blockCount - 1) * TARGET_SPACING;
    
    // Limit the re-adjustment to 3x or 0.33x
    // We don't want to increase/decrease diff too much.
    if (nActualTimespan < nTargetTimespan/3.0f)
        nActualTimespan = nTargetTimespan/3.0f;
    if (nActualTimespan > nTargetTimespan*3.0f)
        nActualTimespan = nTargetTimespan*3.0f;
    
    // Calculate the new difficulty based on actual and target timespan.
    darkTarget = divide(multiplyThis32(darkTarget,nActualTimespan),((UInt256) { .u64 = { nTargetTimespan, 0, 0, 0 } }));
    
    uint32_t compact = getCompact(darkTarget);
    
    // If calculated difficulty is lower than the minimal diff, set the new difficulty to be the minimal diff.
    if (compact > MAX_PROOF_OF_WORK){
        compact = MAX_PROOF_OF_WORK;
    }
    
    // Return the new diff.
    return compact;
}

// frees memory allocated by BRMerkleBlockParse
void BRMerkleBlockFree(BRMerkleBlock *block)
{
    assert(block != NULL);
    
    if (block->hashes) free(block->hashes);
    if (block->flags) free(block->flags);
    free(block);
}
