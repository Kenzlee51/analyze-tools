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
 * Project:      Flash Device Description for Atmel DataFlash AT45DB642D (SPI)
 * -------------------------------------------------------------------- */

#define FLASH_SECTOR_COUNT      64          /* Number of Sectors */
#define FLASH_SECTOR_SIZE       0           /* FLASH_SECTORS information used */
#define FLASH_PAGE_SIZE         1056        /* Programming page size in bytes */
#define FLASH_PROGRAM_UNIT      1           /* Smallest programmable unit in bytes */
#define FLASH_ERASED_VALUE      0xFF        /* Contents of erased memory */

#define FLASH_SECTORS                                               \
  ARM_FLASH_SECTOR_INFO(0x000000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x021000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x042000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x063000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x084000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x0A5000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x0C6000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x0E7000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x108000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x129000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x14A000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x16B000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x18C000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x1AD000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x1CE000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x1EF000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x210000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x231000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x252000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x273000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x294000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x2B5000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x2D6000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x2F7000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x318000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x339000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x35A000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x37B000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x39C000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x3BD000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x3DE000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x3FF000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x420000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x441000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x462000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x483000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x4A4000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x4C5000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x4E6000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x507000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x528000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x549000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x56A000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x58B000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x5AC000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x5CD000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x5EE000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x60F000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x630000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x651000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x672000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x693000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x6B4000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x6D5000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x6F6000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x717000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x738000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x759000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x77A000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x79B000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x7BC000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x7DD000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x7FE000, 0x21000), /* Sector size 132kB */ \
  ARM_FLASH_SECTOR_INFO(0x81F000, 0x21000)  /* Sector size 132kB */
