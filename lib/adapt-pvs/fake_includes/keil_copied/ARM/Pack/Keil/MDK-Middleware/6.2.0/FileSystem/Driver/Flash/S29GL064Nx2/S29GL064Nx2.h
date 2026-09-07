/* -----------------------------------------------------------------------------
 * Copyright (c) 2013 - 2014 ARM Ltd.
 *
 * This software is provided 'as-is', without any express or implied warranty. 
 * In no event will the authors be held liable for any damages arising from 
 * the use of this software. Permission is granted to anyone to use this 
 * software for any purpose, including commercial applications, and to alter 
 * it and redistribute it freely, subject to the following restrictions:
 *
 * 1. The origin of this software must not be misrepresented; you must not 
 *    claim that you wrote the original software. If you use this software in
 *    a product, an acknowledgment in the product documentation would be 
 *    appreciated but is not required. 
 * 
 * 2. Altered source versions must be plainly marked as such, and must not be 
 *    misrepresented as being the original software. 
 * 
 * 3. This notice may not be removed or altered from any source distribution.
 *
 *
 * $Date:        31. March 2014
 * $Revision:    V1.00
 *  
 * Driver:       Driver_Flash# (default: Driver_Flash0)
 * Project:      Flash Device Description for S29GL064Nx2 (32-bit Bus)
 * -------------------------------------------------------------------- */

#define FLASH_SECTOR_COUNT      128         /* Number of Sectors */
#define FLASH_SECTOR_SIZE       0           /* FLASH_SECTORS information used */
#define FLASH_PAGE_SIZE         4           /* Programming page size in bytes */
#define FLASH_PROGRAM_UNIT      4           /* Smallest programmable unit in bytes */
#define FLASH_ERASED_VALUE      0xFF        /* Contents of erased memory */

#define FLASH_SECTORS                                                   \
  ARM_FLASH_SECTOR_INFO(0x000000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x010000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x020000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x030000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x040000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x050000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x060000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x070000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x080000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x090000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x0A0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x0B0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x0C0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x0D0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x0E0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x0F0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x100000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x110000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x120000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x130000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x140000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x150000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x160000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x170000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x180000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x190000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x1A0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x1B0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x1C0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x1D0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x1E0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x1F0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x200000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x210000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x220000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x230000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x240000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x250000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x260000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x270000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x280000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x290000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x2A0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x2B0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x2C0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x2D0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x2E0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x2F0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x300000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x310000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x320000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x330000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x340000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x350000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x360000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x370000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x380000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x390000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x3A0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x3B0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x3C0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x3D0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x3E0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x3F0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x400000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x410000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x420000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x430000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x440000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x450000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x460000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x470000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x480000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x490000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x4A0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x4B0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x4C0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x4D0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x4E0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x4F0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x500000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x510000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x520000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x530000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x540000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x550000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x560000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x570000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x580000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x590000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x5A0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x5B0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x5C0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x5D0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x5E0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x5F0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x600000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x610000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x620000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x630000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x640000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x650000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x660000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x670000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x680000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x690000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x6A0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x6B0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x6C0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x6D0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x6E0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x6F0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x700000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x710000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x720000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x730000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x740000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x750000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x760000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x770000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x780000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x790000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x7A0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x7B0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x7C0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x7D0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x7E0000*2, 0x20000),   /* Sector size 128kB */ \
  ARM_FLASH_SECTOR_INFO(0x7F0000*2, 0x20000)    /* Sector size 128kB */
