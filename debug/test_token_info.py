import os
import asyncio

os.environ['DEBUG_VERBOSE'] = '1'

from helpers.multibuy_logic import get_token_info

async def main():
    mints = [
        'So11111111111111111111111111111111111111112',  # SOL
        'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',  # USDC
        'DezXK6uoVkM4sBuxab4ZpAcLRwE6QWhfFVto8Khp5t1G',  # BONK
    ]
    for mint in mints:
        info = await get_token_info(mint)
        print(mint, info)

if __name__ == '__main__':
    asyncio.run(main()) 