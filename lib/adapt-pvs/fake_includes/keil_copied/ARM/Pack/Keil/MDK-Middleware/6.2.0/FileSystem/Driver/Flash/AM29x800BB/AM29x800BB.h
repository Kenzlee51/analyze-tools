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
 * Project:      Flash Device Description for AM29x800BB (16-bit Bus)
 * -------------------------------------------------------------------- */

#define FLASH_SECTOR_COUNT      22          /* Number of sectors */
#define FLASH_SECTOR_SIZE       0           /* FLASH_SECTORS information used */
#define FLASH_PAGE_SIZE         2           /* Programming page size in bytes */
#define FLASH_PROGRAM_UNIT      2           /* Smallest programmable unit in bytes */
#define FLASH_ERASED_VALUE      0xFF        /* Contents of erased memory */

#define FLASH_SECTORS                                              \
  ARM_FLASH_SECTOR_INFO(0x000000, 0x04000), /* Sector size 16kB */ \
  ARM_FLASH_SECTOR_INFO(0x004000, 0x08000), /* Sector size 32kB */ \
  ARM_FLASH_SECTOR_INFO(0x00C000, 0x02000), /* Sector size  8kB */ \
  ARM_FLASH_SECTOR_INFO(0x00E000, 0x02000), /* Sector size  8kB */ \
  ARM_FLASH_SECTOR_INFO(0x010000, 0x02000), /* Sector size  8kB */ \
  ARM_FLASH_SECTOR_INFO(0x012000, 0x02000), /* Sector size  8kB */ \
  ARM_FLASH_SECTOR_INFO(0x014000, 0x08000), /* Sector size 32kB */ \
  ARM_FLASH_SECTOR_INFO(0x01C000, 0x04000), /* Sector size 16kB */ \
  ARM_FLASH_SECTOR_INFO(0x020000, 0x10000), /* Sector size 64kB */ \
  ARM_FLASH_SECTOR_INFO(0x030000, 0x10000), /* Sector size 64kB */ \
  ARM_FLASH_SECTOR_INFO(0x040000, 0x10000), /* Sector size 64kB */ \
  ARM_FLASH_SECTOR_INFO(0x050000, 0x10000), /* Sector size 64kB */ \
  ARM_FLASH_SECTOR_INFO(0x060000, 0x10000), /* Sector size 64kB */ \
  ARM_FLASH_SECTOR_INFO(0x070000, 0x10000), /* Sector size 64kB */ \
  ARM_FLASH_SECTOR_INFO(0x080000, 0x10000), /* Sector size 64kB */ \
  ARM_FLASH_SECTOR_INFO(0x090000, 0x10000), /* Sector size 64kB */ \
  ARM_FLASH_SECTOR_INFO(0x0A0000, 0x10000), /* Sector size 64kB */ \
  ARM_FLASH_SECTOR_INFO(0x0B0000, 0x10000), /* Sector size 64kB */ \
  ARM_FLASH_SECTOR_INFO(0x0C0000, 0x10000), /* Sector size 64kB */ \
  ARM_FLASH_SECTOR_INFO(0x0D0000, 0x10000), /* Sector size 64kB */ \
  ARM_FLASH_SECTOR_INFO(0x0E0000, 0x10000), /* Sector size 64kB */ \
  ARM_FLASH_SECTOR_INFO(0x0F0000, 0x10000)  /* Sector size 64kB */
