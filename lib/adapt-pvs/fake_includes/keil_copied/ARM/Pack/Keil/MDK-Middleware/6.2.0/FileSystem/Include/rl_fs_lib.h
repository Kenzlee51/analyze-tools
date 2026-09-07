/*------------------------------------------------------------------------------
 * MDK Middleware - Component ::File System
 * Copyright (c) 2004-2014 ARM Germany GmbH. All rights reserved.
 *------------------------------------------------------------------------------
 * Name:    rl_fs_lib.h
 * Purpose: File System Library Definitions
 * Rev.:    V6.2
 *----------------------------------------------------------------------------*/

#ifndef __RL_FS_LIB_H
#define __RL_FS_LIB_H

#include "cmsis_os.h"
#include "Driver_Flash.h"
#include "Driver_MCI.h"
#include "Driver_NAND.h"
#include "Driver_SPI.h"
#include "rl_fs.h"

/* Device System Type attribute */
#define FS_CUR                   0x01   /* Initial Current Drive attribute    */
#define FS_EMB                   0x02   /* Embedded File System (F:)          */
#define FS_FAT                   0x04   /* FAT File System      (M:,N:,R:,U:) */
#define FS_IOC                   0x08   /* IO Control allowed   (M:,N:)       */

/* Driver Init/Uninit Parameter Codes */
#define DM_IO                       0   /* Init/Uninit IO peripherals         */
#define DM_MEDIA                    1   /* Init/Uninit storage Media          */

/* NAND Configuration macros */
#define NAND_ROW_CYCLES(b,p)  ((b*p > 65536) ? 3 : 2)
#define NAND_COL_CYCLES(s)    ((s   >   528) ? 2 : 1)
#define NAND_BTT_SIZE(b)      ((b*3)/100)
#define NAND_SEC_PAGE(p)      (p/512)
#define NAND_SEC_BLOCK(b,p)   ((p/512)*b)
#define _DS_(b)               (b-(b*3)/100-1)
#define _EP_(p)               ((p/512)*128)
#define NAND_TSN_SIZE(b,p)    ((_DS_(b)+_EP_(p)-1)/_EP_(p))

#ifdef __cplusplus
extern "C"  {
#endif

/* Device driver mapping type */
typedef struct {
  void   *dcb;                          /* Device Control block               */
  char    id[3];                        /* Drive Name Identification          */
  uint8_t attr;                         /* File System attributes             */
} const FS_DEV;

/* RAM interface control block */
typedef struct {
  uint8_t *Base;                        /* RAM buffer base address            */
  uint32_t Size;                        /* RAM buffer size                    */
} RAM_DEV;

/* Memory Card Device Control block (MCI) */
typedef struct {
  ARM_DRIVER_MCI       *DrvMci;         /* Registered MCI driver              */
  ARM_MCI_SignalEvent_t MCI_Callback;   /* MCI Driver Callback                */
  ARM_MCI_CAPABILITIES  MCI_Capab;      /* MCI Driver Capabilities            */
  uint8_t              *ExtCSD;         /* Pointer to 512 byte buffer         */
  uint32_t              Property;       /* Card Properties                    */
  uint32_t              SerialNumber;   /* Memory card serial number          */
  uint32_t              SectorCount;    /* Device density in 512B sectors     */
  uint8_t volatile      Event;          /* Driver event flags                 */
  uint8_t volatile      MediaStatus;    /* Media status                       */
  uint8_t               Status;         /* Device status                      */
  uint16_t              RCA;            /* Relative Card Address              */
  uint8_t               Instance;       /* Memory Card Instance number        */
} MC_MCI;

/* Memory Card Device Control block (SPI) */
typedef struct {
  ARM_DRIVER_SPI       *DrvSpi;         /* Registered SPI driver              */
  ARM_SPI_SignalEvent_t SPI_Callback;   /* SPI Driver Callback                */
  ARM_SPI_CAPABILITIES  SPI_Capab;      /* SPI Driver Capabilities            */
  uint8_t              *ExtCSD;         /* Pointer to 512 byte buffer         */
  uint32_t              Property;       /* Card Properties                    */
  uint32_t              SerialNumber;   /* Memory card serial number          */
  uint32_t              SectorCount;    /* Device density in 512B sectors     */
  uint8_t volatile      Event;          /* Driver event flags                 */
  uint8_t volatile      MediaStatus;    /* Media status                       */
  uint8_t               Status;         /* Device status                      */
  uint16_t              RCA;            /* Relative Card Address              */
  uint8_t               Instance;       /* Memory Card Instance number        */
} MC_SPI;

/* NAND Page Layout configuration */
typedef struct _NAND_PAGE_LAYOUT {
  struct {
    uint8_t  ofs_lsn;                   /* LSN position, where logical sector number (LSN) is placed. Usually, this is the first byte of Spare and has the value 0. LSN is a 32-bit value. */
    uint8_t  ofs_dcm;                   /* Page Data corrupted marker. Usually, this byte is the 5-th byte of Spare and has the value 4. */
    uint8_t  ofs_bbm;                   /* Bad Block marker position. Is usually placed as the 6-th byte of Spare and has the value 5. */
    uint8_t  ofs_ecc;                   /* Position of the first byte of Error Correction Code (ECC). Is usually the 7-th byte of Spare and has the value 6. This value is used by flash translation layer only if ECC is encoded and decoded in software. */
  } spare;                              /* Structure spare */
  uint16_t spare_ofs;                   /* Spare area offset from beginning of the page. */
  uint16_t spare_inc;                   /* Column increment till next spare. If page contains multiple sectors, then the first byte of the first spare area is determined by reading spare_ofs value. Location of the first byte of the second spare is (spare_inc + spare_ofs). */
  uint16_t sector_inc;                  /* Column increment till next sector. If page contains multiple sectors, then the first sector always starts at the beginning of the page (byte zero). Second sector starts at sect_inc, third sector at (sect_inc + sect_inc) and so on. */
} NAND_PAGE_LAYOUT;

/* NAND Flash Device information */
typedef struct _NAND_DEVICE {
  NAND_PAGE_LAYOUT *page_layout;        /* Page Layout configuration          */
  uint8_t           bus_width;          /* Device bus width                   */
  uint8_t           device_number;      /* Device number (chip select)        */
  uint16_t          page_size;          /* Page Size in bytes                 */
  uint32_t          block_count;        /* Number of Blocks in Device         */
  uint16_t          page_count;         /* Number of Pages per Block          */
  uint16_t          block_sectors;      /* Number of Sectors per Block        */
  uint8_t           page_sectors;       /* Number of Sectors per Page         */
  uint8_t           row_cycles;         /* Number of Row address cycles       */
  uint8_t           col_cycles;         /* Number of Column address cycles    */
  uint8_t           sw_ecc;             /* value > 0: error correction code (ECC) encoding/decoding enabled in software: \n value = 0: Software ECC disabled \n value = 1: Hamming ECC algorithm enabled in software. */
} NAND_DEVICE;

/* NAND Driver Info */
typedef struct _NAND_DRIVER_INFO {
  ARM_NAND_SignalEvent_t hw_cb;         /* Hardware event handler (callback)  */
  ARM_DRIVER_NAND       *drv;           /* NAND driver                        */
  ARM_NAND_CAPABILITIES  capabilities;  /* NAND driver capabilities           */
  uint8_t                init;          /* Initialization counter             */
} NAND_HW_DRIVER;

/* Signal NAND Media Driver event */
typedef void (*NAND_Media_SignalEvent_t) (uint32_t dev_num, uint32_t event);

/* NAND Media Driver Instance */
typedef struct _NFTL_DRIVER_HANDLE {
  NAND_Media_SignalEvent_t media_cb;    /* NAND Media event handler (callback)*/
  NAND_HW_DRIVER          *hw;          /* Hardware driver handle             */
  NAND_DEVICE const       *dev;         /* NAND Device description            */
  uint8_t volatile         event;       /* Event flags                        */
  uint8_t                  status;      /* NAND Media Layer status flags      */
  uint16_t                 ecc;         /* ECC algorithm index and ECC flags  */
} NAND_MEDIA_HANDLE;

/* NAND Media Interface Driver */
typedef struct _NAND_MEDIA_DRIVER {
  int32_t (*Initialize)     (NAND_MEDIA_HANDLE *h);
  int32_t (*Uninitialize)   (NAND_MEDIA_HANDLE *h);
  int32_t (*ResetDevice)    (NAND_MEDIA_HANDLE *h);
  int32_t (*ReadID)         (NAND_MEDIA_HANDLE *h, uint8_t addr, uint8_t *buf, uint32_t len);
  int32_t (*ReadParamPage)  (NAND_MEDIA_HANDLE *h, uint32_t col, uint8_t *buf, uint32_t len);
  int32_t (*ReadPage)       (NAND_MEDIA_HANDLE *h, uint32_t row, uint32_t col, uint8_t *buf, uint32_t len);
  int32_t (*WritePage)      (NAND_MEDIA_HANDLE *h, uint32_t row, uint32_t col, const uint8_t *buf, uint32_t len);
  int32_t (*CopyPage)       (NAND_MEDIA_HANDLE *h, uint32_t row_src, uint32_t row_dst, uint32_t row_cnt);
  int32_t (*EraseBlock)     (NAND_MEDIA_HANDLE *h, uint32_t row);
  int32_t (*ReadStatus)     (NAND_MEDIA_HANDLE *h, uint8_t *stat);
} const NAND_MEDIA_DRIVER;

/* NAND FTL Block Index Cache */
typedef struct {
  uint16_t pbn;                         /* Physical block number              */
  uint16_t lbn;                         /* Logical block number               */
  uint8_t  typ;                         /* Type of block in cache             */
  uint8_t  nextPg;                      /* Next page to be indexed            */
  uint8_t  *pgIdx;                      /* Index buffer                       */
} BLOCK_CACHE;

/* NAND FTL Page Cache */
typedef struct {
  uint32_t row;                         /* Row address of the page in cache   */
  uint8_t *buf;                         /* Cached page buffer                 */
} PAGE_CACHE;

/* NAND FTL Buffer Pointers Structure */
typedef struct {
  uint32_t CacheBS;                     /* Block slot for block caching       */
  uint32_t CachePS;                     /* Page slot for page caching         */
  uint16_t CachedBlocks;                /* Number of indexed blocks           */
  uint16_t CachedPages;                 /* Number of cached pages             */
  BLOCK_CACHE *Block;                   /* Block indexing cache info struct   */
  PAGE_CACHE  *Page;                    /* Page data cache info structure     */
} NAND_FTL_CACHE;

/* NAND FTL Configuration structure */
typedef struct {
  /* Block Translation Table Space */
  uint16_t BttStartBn;                  /* First Physical Block               */
  uint16_t BttEndBn;                    /* Last Physical Block                */
  /* Data Space */
  uint16_t DataStartBn;                 /* First Physical Block               */
  uint16_t DataEndBn;                   /* Last Physical Block                */
  /* Buffer Sizes */  
  uint16_t NumCacheBlocks;              /* Number of indexed data blocks      */
  uint16_t NumCachePages;               /* Number of cached data pages        */
  uint16_t TsnTableSize;                /* Translation table cache size       */
  /* Page buffer & Caches */
  uint8_t     *PgBuf;                   /* Page data buffer                   */
  BLOCK_CACHE *BlockCache;              /* Block indexing cache info struct   */
  uint8_t     *BlockCacheBuf;           /* Block indexing cache buffer        */
  PAGE_CACHE  *PageCache;               /* Page data cache info structure     */
  uint8_t     *PageCacheBuf;            /* Page data cache buffer             */
  uint32_t    *TsnTable;                /* Translation table cache buffer     */
} const NAND_FTL_CFG;

/* NAND Device Control block */
typedef struct {
  NAND_MEDIA_HANDLE *Media;             /* NAND Media Driver Instance         */
  NAND_FTL_CFG      *Cfg;               /* NAND FTL configuration structure   */
  NAND_PAGE_LAYOUT   PgLay;             /* Page layout definition             */

  uint8_t   PageSectors;                /* Number of sectors/page 1,4,8, ...  */
  uint8_t   SPP;                        /* Number of sectors/page             */
                                        /*  (2^n, n = SPP)                    */
  uint8_t   PPB;                        /* Number of pages/block              */
                                        /*  (2^n, n = PPB)                    */
  uint8_t   SPB;                        /* Number of sectors/block            */
                                        /*  (2^n, n = SPB)                    */
  uint8_t   EPS;                        /* Entries/table sector               */
                                        /*  (2^n, n = EPS)                    */
  uint8_t   LastECC;                    /* ECC status after last page read    */
  uint16_t  LastDBN;                    /* Last allocated phy. block          */
                                        /*  in data area                      */
  uint16_t  LastTBN;                    /* Last allocated phy. block          */
                                        /*  in table area                     */
  uint32_t  CurrLBN;                    /* Current logical block number       */
  uint32_t  GcLBN;                      /* Current logical block number used  */
                                        /*  by forced GC                      */
  uint16_t  PbnQ[3];                    /* Empty block queue                  */
  uint16_t  BadBlockCnt;                /* Bad Block Counter                  */
  uint16_t  NumDataBlocks;              /* Number of data blocks reported to  */
                                        /*  file system                       */
  uint8_t   Status;                     /* FTL Status Flags                   */
  volatile
  uint8_t   Timeout;                    /* Timeout status                     */
  uint8_t  *PgBuf;                      /* Page data buffer                   */
  uint32_t *TsnTable;                   /* Translation table cache buffer     */
  uint16_t  TsnTableSize;               /* Translation table cache size       */
  NAND_FTL_CACHE Ca;                    /* FTL cache info structure           */
} NAND_FTL_DEV;

/* FAT Name Cache Configuration */
typedef struct fat_nca_cfg {
  uint8_t   MaxPathDepth;               /* Maximum path depth                 */
  uint8_t   ControlBlockCnt;            /* Number of control blocks           */
  uint32_t  NameMemPoolSize;            /* Name cache memory pool size        */
  uint32_t *NameMemPool;                /* Name cache memory pool             */
} const FAT_NCACHE_CFG;

/* FAT Sector Caching structure */
typedef struct fcache {
  uint32_t sect;                        /* Cached FAT sector number           */
  uint8_t  *buf;                        /* FAT sector cache buffer            */
  uint8_t  dirty;                       /* FAT table content modified         */
  uint8_t  cfat;                        /* Current FAT                        */
} FCACHE;

/* Data Sector Caching structure */
typedef struct dcache {
  uint32_t sect;                        /* Working Data sector number         */
  uint32_t csect;                       /* Data Cache starting sector number  */
  uint8_t  *buf;                        /* Working Data sector buffer         */
  uint8_t  *cbuf;                       /* Data Cache sector buffer           */
  uint8_t  nwr;                         /* Number of buffered write sectors   */
  uint8_t  nrd;                         /* Number of cached read sectors      */
} DCACHE;

/* Name Caching structure */
typedef struct _NCACHE {
  uint8_t   max_path_depth;             /* Maximum path depth                 */
  uint32_t  size;                       /* Name cache memory pool size        */
  uint32_t *buf;                        /* Name cache memory pool             */
} NCACHE;

/* FAT Volume Information struct */
typedef struct fatinfo {
  uint32_t BootRecSec;                  /* Boot Record Sector Offset          */
  uint8_t  FatType;                     /* File System Fat type.              */
  uint8_t  NumOfFat;                    /* Number of Fat tables               */
  uint8_t  SecPerClus;                  /* Number of Sectors per Cluster      */
  uint16_t FatSize;                     /* Number of Sectors per FAT table    */
  uint16_t RsvdSecCnt;                  /* Number of Reserved Sectors         */
  uint32_t DskSize;                     /* Disk Size Sector Count             */
  uint32_t DataSecCnt;                  /* Number of Data Sectors             */
  uint16_t RootEntCnt;                  /* Maximum Root Directory entries     */
  uint16_t BytesPerSec;                 /* Sector Size in bytes               */
  uint32_t DataClusCnt;                 /* Data Cluster Count                 */
  uint32_t RootDirAddr;                 /* Root Dir First Sector              */
  uint16_t RootSecCnt;                  /* Number of Sectors for Root dir     */
  uint32_t ClusSize;                    /* Cluster Size in bytes              */
  uint16_t EntsPerClus;                 /* Number of entries per cluster      */
                                        /* Added fields for FAT32             */
  uint16_t FAT32_ExtFlags;              /* FAT extended flags                 */
  uint32_t FAT32_RootClus;              /* Root directory first cluster       */
  uint16_t FAT32_FSInfo;                /* FSInfo structure sector number     */
  uint16_t FAT32_BkBootSec;             /* Boot Record copy sector number     */
} FATINFO;

/* FAT File System driver */
typedef struct {
  uint32_t (*Init)        (uint32_t mode);
  uint32_t (*UnInit)      (uint32_t mode);
  uint32_t (*ReadSect)    (uint32_t sect, uint8_t *buf, uint32_t cnt);
  uint32_t (*WriteSect)   (uint32_t sect, uint8_t *buf, uint32_t cnt);
  uint32_t (*ReadInfo)    (fsMediaInfo *cfg);
  fsStatus (*DeviceCtrl)  (fsDevCtrlCode code, void *p);
} const FAT_DRV;

/* File System Journal Control block */
typedef struct fsjour {
  FAT_DRV *drv;                         /* Memory device driver               */
  uint8_t *buf;                         /* Journal buffer                     */
  uint32_t FirstSect;                   /* First sector of journal            */
  uint32_t JournSect;                   /* Number of sectors available        */
  uint32_t TrId;                        /* Transaction identifier             */
  uint32_t TrSect;                      /* Transaction sector                 */
  uint8_t  State;                       /* Journal state                      */
  uint8_t  Status;                      /* Journal status                     */
} FSJOUR;

/* FAT Volume Description */
typedef struct _fsFAT_Volume {
  FAT_DRV  *Drv;                        /* Registered FAT Device Driver       */
  uint32_t  Status;                     /* Volume Status                      */
  uint32_t *CaBuf;                      /* Cache Buffer (FAT + Data)          */
  uint32_t  CaSize;                     /* Cache Buffer size                  */
  uint16_t  RsvdS;                      /* Reserved sectors used by journal   */
  FSJOUR   *fsj;                        /* File System Journal                */
  FATINFO   cfg;                        /* FAT Volume configuration           */
  FCACHE    fat;                        /* FAT table cache control            */
  DCACHE    ca;                         /* Data cache control                 */
  uint32_t  free_clus_cnt;              /* FAT32: Number of free clusters     */
  uint32_t  free_clus;                  /* FAT32: First free cluster          */
  uint32_t  cdir_clus;                  /* Current directory cluster          */
  NCACHE   *ncache;                     /* Name cache memory                  */
} fsFAT_Volume;

/* FAT File Handle Description */
typedef struct _fsFAT_Handle {
  fsFAT_Volume *vol;                    /* FAT volume pointer                 */
  uint16_t  flags;                      /* File status flags                  */
  uint32_t  fsize;                      /* File size                          */
  uint32_t  fcsz;                       /* Current file size                  */
  uint32_t  fpos;                       /* File position pointer              */

  uint32_t  short_ent_clus;             /* SFN: Short entry cluster number    */
  uint16_t  short_ent_offs;             /* SFN: Short entry sector offset     */

  uint32_t  first_clus;                 /* First data cluster                 */
  uint32_t  current_clus;               /* Current data cluster               */
  uint8_t   current_sect;               /* Current data sector                */
} fsFAT_Handle;

/* EFS Volume Description */
typedef struct _fsEFS_Volume {
  ARM_DRIVER_FLASH       *Drv;          /* Registered Device Driver           */
  ARM_FLASH_SECTOR       *DevSect;      /* Flash Sectors Array                */
  ARM_FLASH_CAPABILITIES  Capab;        /* Flash driver capabilities          */
  ARM_Flash_SignalEvent_t Callback;     /* Flash driver callback              */
  uint32_t volatile       Event;        /* Flash driver events                */
  uint32_t                Status;       /* Volume Status                      */
  uint32_t                InitVal;      /* Value for empty flash              */
  uint32_t                NumSect;      /* Number of Flash Sectors            */
  uint32_t                Size;         /* Device Size                        */
  uint16_t                TopID;        /* Top used FileID                    */
} fsEFS_Volume;

/* EFS File Handle Description */
typedef struct _fsEFS_Handle {
  fsEFS_Volume *vi;                     /* Volume Information                 */
  uint16_t  flags;                      /* File status flags                  */
  uint32_t  fsize;                      /* File Size                          */
  uint32_t  fpos;                       /* File Position Indicator            */

  uint16_t  fileID;                     /* File Identification Number         */
  uint16_t  fblock;                     /* Current Flash Block index          */
  uint16_t  fidx;                       /* Current File Block index           */
  uint16_t  nfidx;                      /* Next File Block index              */
  uint32_t  fbot;                       /* Flash Block free space bottom      */
  uint32_t  ftop;                       /* Flash Block free space top         */
} fsEFS_Handle;

/* File System Configuration function */
extern void     fs_config          (const char *drive);

/* RAM drive interface functions */
extern uint32_t ram_Init           (uint32_t mode, RAM_DEV *ram);
extern uint32_t ram_UnInit         (uint32_t mode, RAM_DEV *ram);
extern uint32_t ram_ReadSector     (uint32_t sect, uint8_t *buf, uint32_t cnt, RAM_DEV *ram);
extern uint32_t ram_WriteSector    (uint32_t sect, uint8_t *buf, uint32_t cnt, RAM_DEV *ram);
extern uint32_t ram_ReadInfo       (fsMediaInfo *info, RAM_DEV *ram);
extern fsStatus ram_DevCtrl        (fsDevCtrlCode code, void *p, RAM_DEV *ram);

/* Memory Card SPI mode interface functions */
extern uint32_t mc_spi_Init        (uint32_t mode, MC_SPI *mc);
extern uint32_t mc_spi_UnInit      (uint32_t mode, MC_SPI *mc);
extern uint32_t mc_spi_ReadSector  (uint32_t sect, uint8_t *buf, uint32_t cnt, MC_SPI *mc);
extern uint32_t mc_spi_WriteSector (uint32_t sect, uint8_t *buf, uint32_t cnt, MC_SPI *mc);
extern uint32_t mc_spi_ReadInfo    (fsMediaInfo *info, MC_SPI *mc);
extern fsStatus mc_spi_DevCtrl     (fsDevCtrlCode code, void *p, MC_SPI *mc);
extern void     mc_spi_DrvEvent    (uint32_t event, MC_SPI *mc);

/* Memory Card Native mode interface functions */
extern uint32_t mc_mci_Init        (uint32_t mode, MC_MCI *mc);
extern uint32_t mc_mci_UnInit      (uint32_t mode, MC_MCI *mc);
extern uint32_t mc_mci_ReadSector  (uint32_t sect, uint8_t *buf, uint32_t cnt, MC_MCI *mc);
extern uint32_t mc_mci_WriteSector (uint32_t sect, uint8_t *buf, uint32_t cnt, MC_MCI *mc);
extern uint32_t mc_mci_ReadInfo    (fsMediaInfo *info, MC_MCI *mc);
extern fsStatus mc_mci_DevCtrl     (fsDevCtrlCode code, void *p, MC_MCI *mc);
extern void     mc_mci_DrvEvent    (uint32_t event, MC_MCI *mc);

/* NAND Flash Translation Layer interface functions */
extern uint32_t ftl_Init           (uint32_t mode, NAND_FTL_DEV *ftl);
extern uint32_t ftl_UnInit         (uint32_t mode, NAND_FTL_DEV *ftl);
extern uint32_t ftl_ReadSect       (uint32_t lsn, uint8_t *buf, uint32_t cnt, NAND_FTL_DEV *ftl);
extern uint32_t ftl_WriteSect      (uint32_t lsn, uint8_t *buf, uint32_t cnt, NAND_FTL_DEV *ftl);
extern uint32_t ftl_ReadInfo       (fsMediaInfo *info, NAND_FTL_DEV *ftl);
extern fsStatus ftl_DevCtrl        (fsDevCtrlCode code, void *p, NAND_FTL_DEV *ftl);
extern void     ftl_DrvEvent       (NAND_FTL_DEV *ftl);

/* USB Mass Storage interface functions */
extern bool     FS_USBH_MSC_Initialize  (uint8_t instance, uint32_t mode);
extern bool     FS_USBH_MSC_Uninitialize(uint8_t instance, uint32_t mode);
extern bool     FS_USBH_MSC_ReadSectors (uint8_t instance, uint32_t sector_addr, uint32_t sector_num, uint8_t *buf);
extern bool     FS_USBH_MSC_WriteSectors(uint8_t instance, uint32_t sector_addr, uint32_t sector_num, uint8_t *buf);
extern bool     FS_USBH_MSC_ReadInfo    (uint8_t instance, fsMediaInfo *info);
extern fsStatus FS_USBH_MSC_DeviceCtrl  (uint8_t instance, fsDevCtrlCode code, void *p);

/* File System Journal interface functions */
extern uint32_t fsj_init      (FSJOUR *fsj, FAT_DRV *drv);
extern uint32_t fsj_set_space (FSJOUR *fsj, uint32_t start_sect, uint32_t cnt);
extern uint32_t fsj_write     (FSJOUR *fsj, uint32_t sect, uint32_t rsec, uint8_t *buf);

/* Embedded File System interface functions */
extern fsStatus efs_init      (fsEFS_Volume *vol);
extern fsStatus efs_uninit    (fsEFS_Volume *vol);
extern fsStatus efs_mount     (fsEFS_Volume *vol);
extern fsStatus efs_unmount   (fsEFS_Volume *vol);
extern fsStatus efs_open      (int32_t handle, const char *fn, int openmode);
extern fsStatus efs_close     (int32_t handle);
extern int32_t  efs_read      (int32_t handle, uint8_t *buf, uint32_t len);
extern int32_t  efs_write     (int32_t handle, const uint8_t *buf, uint32_t len);
extern fsStatus efs_flush     (int32_t handle);
extern int32_t  efs_flen      (int32_t handle);
extern fsStatus efs_seek      (int32_t handle, uint32_t pos);

/* EFS Auxiliary */
extern fsStatus efs_create    (int32_t handle, const char *name);
extern fsStatus efs_find      (int32_t handle, const char *name);

/* EFS Maintenance Routines */
extern fsStatus efs_delete    (int32_t handle);
extern fsStatus efs_ffind     (int32_t handle, fsFileInfo *info);
extern fsStatus efs_rename    (int32_t handle, const char *filename);

/* EFS Utility Routines */
extern uint32_t efs_free      (fsEFS_Volume *vi);
extern fsStatus efs_format    (fsEFS_Volume *vi);
extern int32_t  efs_analyse   (fsEFS_Volume *vi);
extern fsStatus efs_check     (fsEFS_Volume *vi);
extern fsStatus efs_defrag    (fsEFS_Volume *vi);

/* EFS Event Callback handler for driver events */
extern void     efs_DrvEvent  (uint32_t event, fsEFS_Volume *vol);

/* FAT File System interface functions */
extern fsStatus fat_init      (fsFAT_Volume *vol);
extern fsStatus fat_uninit    (fsFAT_Volume *vol);
extern fsStatus fat_mount     (fsFAT_Volume *vol);
extern fsStatus fat_unmount   (fsFAT_Volume *vol);
extern fsStatus fat_open      (int32_t handle, const char *name, int openmode);
extern fsStatus fat_close     (int32_t handle);
extern int32_t  fat_read      (int32_t handle, uint8_t *buf, uint32_t len);
extern int32_t  fat_write     (int32_t handle, const uint8_t *buf, uint32_t len);
extern int32_t  fat_flen      (int32_t handle);
extern fsStatus fat_seek      (int32_t handle, uint32_t pos);

/* FAT File Maintenance Routines */
extern fsStatus fat_delete_l  (const char *fn, fsFAT_Volume *vol);
extern fsStatus fat_delete    (const char *fn, const char *options, fsFAT_Volume *vol);
extern fsStatus fat_ffind     (const char *fn, fsFileInfo *info, fsFAT_Volume *vol);
extern fsStatus fat_rename    (const char *path, const char *newname, fsFAT_Volume *vol);

/* FAT Directory Support Routines */
extern fsStatus fat_mkdir     (const char *path, fsFAT_Volume *vol);
extern fsStatus fat_rmdir     (const char *path, const char *options, fsFAT_Volume *vol);
extern fsStatus fat_chdir     (const char *path, fsFAT_Volume *vol);
extern fsStatus fat_pwd       (      char *path, uint32_t len, fsFAT_Volume *vol);

/* FAT Utility Routines */
extern uint64_t fat_free      (fsFAT_Volume *vol);
extern fsStatus fat_format    (fsFAT_Volume *vol, const char *param);

/* FAT Journal System Routines */
extern uint32_t fat_jour_init (fsFAT_Volume *vol);
extern uint32_t fat_jour_prep (fsFAT_Volume *vol);

/* FileSystem Retarget Routines */
extern int      __sys_open    (const char *fname, int openmode);
extern int      __sys_close   (int handle);
extern int      __sys_read    (int handle, uint8_t *buf, uint32_t len);
extern int      __sys_write   (int handle, const uint8_t *buf, uint32_t len);
extern int      __sys_ensure  (int handle);
extern int      __sys_seek    (int handle, uint32_t pos);
extern long     __sys_flen    (int handle);

#ifdef __cplusplus
}
#endif

#endif /* __RL_FS_LIB_H */
