/*------------------------------------------------------------------------------
 * MDK Middleware - Component ::File System
 * Copyright (c) 2004-2014 ARM Germany GmbH. All rights reserved.
 *------------------------------------------------------------------------------
 * Name:    rl_fs.h
 * Purpose: File System API
 * Rev.:    V6.2
 *----------------------------------------------------------------------------*/

#ifndef __RL_FS_H
#define __RL_FS_H

#include <stdint.h>
#include <stdbool.h>

// ==== Enumeration, structures, defines ====

/// FAT File Attribute bit masks.
#define FS_FAT_ATTR_READ_ONLY    0x01   ///< Read-only file attribute
#define FS_FAT_ATTR_HIDDEN       0x02   ///< Hidden file attribute
#define FS_FAT_ATTR_SYSTEM       0x04   ///< System file attribute
#define FS_FAT_ATTR_VOLUME_ID    0x08   ///< Volume ID attribute
#define FS_FAT_ATTR_DIRECTORY    0x10   ///< Directory file attribute
#define FS_FAT_ATTR_ARCHIVE      0x20   ///< Archive file attribute

/// Driver CheckMedia return bit masks.
#define FS_MEDIA_INSERTED        0x01   ///< Media Detected
#define FS_MEDIA_PROTECTED       0x02   ///< Media Write Protected
#define FS_MEDIA_INITIALIZED     0x04   ///< Media Initialized
#define FS_MEDIA_NOCHKMEDIA      0x80   ///< Check Media Not implemented


#ifdef __cplusplus
extern "C"  {
#endif

/// File System return codes.
typedef enum {
  fsOK = 0,                             ///< Operation succeeded
  fsError,                              ///< Unspecified error
  fsAccessDenied,                       ///< Resource access denied
  fsInvalidParameter,                   ///< Invalid parameter specified
  fsInvalidDrive,                       ///< Nonexistent drive
  fsInvalidPath,                        ///< Invalid path specified
  fsUninitializedDrive,                 ///< Drive is uninitialized
  fsDriverError,                        ///< Read/write error
  fsMediaError,                         ///< Media error
  fsNoMedia,                            ///< No media, or not initialized
  fsNoFileSystem,                       ///< File system is not formatted
  fsNoFreeSpace,                        ///< No free space available
  fsFileNotFound,                       ///< Requested file not found
  fsDirNotEmpty,                        ///< The directory is not empty
  fsTooManyOpenFiles                    ///< Too many open files
} fsStatus;

/// File System Type.
typedef enum _fsType {
  fsTypeNone = 0,                       ///< No file system (volume unformatted)
  fsTypeUnknown,                        ///< File system type is unknown
  fsTypeFAT12,                          ///< File system type is FAT12
  fsTypeFAT16,                          ///< File system type is FAT16
  fsTypeFAT32,                          ///< File system type is FAT32
  fsTypeEFS                             ///< File system type is EFS
} fsType;

/// Driver DeviceCtrl parameter code.
typedef enum _fsDevCtrlCode {
  fsDevCtrlCodeCheckMedia = 0,          ///< Check Media Status
  fsDevCtrlCodeFormat,                  ///< Format Media at low level
  fsDevCtrlCodeSerial,                  ///< Return device serial number
  fsDevCtrlCodeGetCID                   ///< Read Memory Card CID Register
} fsDevCtrlCode;

/// Media information.
typedef struct _fsMediaInfo {
  uint32_t  block_cnt;                  ///< Total number of blocks on the volume
  uint16_t  read_blen;                  ///< Read block length
  uint16_t  write_blen;                 ///< Write block length
} fsMediaInfo;

/// IO Control Cache information.
typedef struct _fsIOC_Cache {
  uint8_t *buffer;                      ///< Cache Buffer (4 byte aligned)
  uint32_t size;                        ///< Cache Buffer size in bytes
} fsIOC_Cache;

/// Memory card CID register.
typedef struct _fsCID_Register {
  uint32_t   PSN;                       ///< Product serial number
  uint16_t   OID;                       ///< OEM/Application ID
  uint8_t    MID;                       ///< Manufacturer ID
  uint8_t    PRV;                       ///< Product revision
  uint16_t   MDT;                       ///< Manufacturing date
  uint8_t    PNM[6];                    ///< Product name
} fsCID_Register;

/// File System Time structure.
typedef struct _fsTime {
  uint8_t  hr;                          ///< Hours    [0..23]
  uint8_t  min;                         ///< Minutes  [0..59]
  uint8_t  sec;                         ///< Seconds  [0..59]
  uint8_t  day;                         ///< Day      [1..31]
  uint8_t  mon;                         ///< Month    [1..12]
  uint16_t year;                        ///< Year     [1980..2107]
} fsTime;

/// File information data.
typedef struct _fsFileInfo {
  char     name[256];                   ///< Filename path
  uint32_t size;                        ///< File size in bytes
  uint16_t fileID;                      ///< System Identification
  uint8_t  attrib;                      ///< Attributes
  fsTime   time;                        ///< Create/Modify Time
} fsFileInfo;

/// Drive information.
typedef struct _fsDriveInfo {
  fsType   fs_type;                     ///< Drives file system type
  uint64_t capacity;                    ///< Drives capacity in bytes
} fsDriveInfo;


//  ==== System Routines ====

/// \brief Initialize File System and drive related driver.
/// \param[in]  drive                    a string specifying the \ref drive "memory or storage device".
/// \return     execution status \ref fsStatus
///               - fsOK               = Operation successful.
///               - fsInvalidParameter = Input parameter invalid.
///               - fsInvalidDrive     = Nonexistent drive letter specified.
///               - fsDriverError      = Failed to initialize the driver.
extern fsStatus finit (const char *drive);

/// \brief Uninitialize File System.
/// \param[in]  drive                    a string specifying the \ref drive "memory or storage device".
/// \return     execution status \ref fsStatus
///               - fsOK               = Operation successful.
///               - fsInvalidParameter = Input parameter invalid.
///               - fsInvalidDrive     = Nonexistent drive letter specified.
extern fsStatus funinit (const char *drive);

/// \brief Mount drive.
/// \param[in]  drive                    a string specifying the \ref drive "memory or storage device".
/// \return     execution status \ref fsStatus
///               - fsOK               = Operation successful.
///               - fsInvalidParameter = Input parameter invalid.
///               - fsInvalidDrive     = Nonexistent drive letter specified.
///               - fsDriverError      = Media driver not initialized.
///               - fsMediaError       = Failed to initialize the media.
///               - fsNoFileSystem     = No filesystem on the volume.
extern fsStatus fmount (const char *drive);

/// \brief Unmount drive.
/// \param[in]  drive                    a string specifying the \ref drive "memory or storage device".
/// \return     execution status \ref fsStatus
///               - fsOK               = Operation successful.
///               - fsInvalidParameter = Input parameter invalid.
///               - fsInvalidDrive     = Nonexistent drive letter specified.
extern fsStatus funmount (const char *drive);


//  ==== File Maintenance Routines ====

/// \brief Delete one or multiple files.
/// \param[in]  path                     a string specifying the file or directory.
/// \param[in]  options                  a string specifying function options.
/// \return     execution status \ref fsStatus
#ifdef MW_FILESYSTEM_FDELETE_LEGACY
extern fsStatus fdelete (const char *path);
#else
extern fsStatus fdelete (const char *path, const char *options);
#endif


/// \brief Find a file or directory matching search pattern.
/// \param[in]  pattern                  string specifying the pattern.
///                                      - May include drive prefix and the following wildcards:
///                                      - "*" or "*.*" searches for all files in the directory.
///                                      - "abc*"       searches for files that begin with abc.
///                                      - "*.htm"      searches for files that end with .htm.
///                                      - "abc*.text"  searches for files that begin with abc and that end with .text.
/// \param[out] info                     structure storing information about matching files.
/// \return     execution status \ref fsStatus
///               - fsOK               = Operation successful.
///               - fsInvalidDrive     = Nonexistent drive letter specified.
///               - fsTooManyOpenFiles = File cannot be opened due to too many opened files.
///               - fsError            = File or folder containing specified pattern cannot be found.
extern fsStatus ffind (const char *pattern, fsFileInfo *info);

/// \brief Rename a file or directory with given path name to a new name.
/// \param[in]  path                     string specifying the file or directory path.
/// \param[in]  newname                  string specifying new file or directory name.
/// \return     execution status \ref fsStatus
///               - fsOK               = Operation successful.
///               - fsInvalidPath      = Invalid path specified.
///               - fsInvalidDrive     = Nonexistent drive letter specified.
///               - fsAccessDenied     = File is in use.
///               - fsTooManyOpenFiles = File cannot be opened due to too many opened files.
///               - fsError            = Old file cannot be found or new file already exists.
extern fsStatus frename (const char *path, const char *newname);

/// \brief Change file attributes.
/// \param[in]  path                     string specifying file or directory path.
/// \param[in]  attr                     string specifying file or directory attributes to be modified.
///                                      The following characters are allowed within attr string:
///                                      - + Sets an attribute.
///                                      - - Clears an attribute.
///                                      - R Read-only file attribute.
///                                      - A Archive file attribute.
///                                      - S System file attribute.
///                                      - H Hidden file attribute.
/// \return     execution status \ref fsStatus
///               - fsOK               = Operation successful.
///               - fsInvalidParameter = Input parameters are not valid.
///               - fsInvalidDrive     = Nonexistent drive letter specified.
///               - fsInvalidPath      = Invalid path was specified.
///               - fsTooManyOpenFiles = File cannot be opened due to too many opened files.
///               - fsAccessDenied     = File attributes change is not allowed.
/// \note       This function supports FAT drives only.
extern fsStatus fattrib (const char *path, const char *attr);


//  ==== Directory Maintenance Routines ====

/// \brief Print working directory.
/// \param[in]  drive                    a string specifying the \ref drive "memory or storage device".
/// \param[out] buf                      pointer to string buffer.
/// \param[in]  len                      string buffer length.
/// \return     execution status \ref fsStatus
extern fsStatus fpwd (const char *drive, char *buf, uint32_t len);

/// \brief Change working directory.
/// \param[in]  path                     string specifying the directory path.
/// \return     execution status \ref fsStatus
extern fsStatus fchdir (const char *path);

/// \brief Create a directory.
/// \param[in]  path                     string specifying directory path.
/// \return     execution status \ref fsStatus
extern fsStatus fmkdir (const char *path);

/// \brief Remove a directory.
/// \param[in]  path                     string specifying directory path.
/// \param[in]  options                  string specifying function options.
/// \return     execution status \ref fsStatus
extern fsStatus frmdir (const char *path, const char *options);


//  ==== Utility Routines ====

/// \brief Change current drive.
/// \param[in]  drive                    a string specifying the \ref drive "memory or storage device".
/// \return     execution status \ref fsStatus
extern fsStatus fchdrive (const char *drive);

/// \brief Find free space on drive.
/// \param[in]  drive                    a string specifying the \ref drive "memory or storage device".
/// \return     free space or execution status
///             - value >= 0: free space on drive in bytes.
///             - value < 0:  error occurred, -value is execution status as defined with \ref fsStatus
extern int64_t ffree (const char *drive);

/// \brief Format drive.
/// \param[in]  drive                    a string specifying the \ref drive "memory or storage device".
/// \param[in]  options                  string specifying formatting options.
/// \return     execution status \ref fsStatus
///               - fsOK               = Operation successful.
///               - fsInvalidDrive     = Nonexistent drive letter specified.
///               - fsTooManyOpenFiles = File cannot be opened due to too many opened files.
///               - fsError            = Formatting failed.
extern fsStatus fformat (const char *drive, const char *options);

/// \brief Analyse volume and check for file fragmentation.
/// \param[in]  drive                    a string specifying the \ref drive "memory or storage device".
/// \return     fragmentation factor or execution status
///             - value >= 0 or <= 255: fragmentation factor
///             - value < 0: error occurred, -value is execution status as defined with \ref fsStatus
/// \note       This function supports EFS drives only.
extern int32_t fanalyse (const char *drive);

/// \brief Analyse volume and check for allocation errors.
/// \param[in]  drive                    a string specifying the \ref drive "memory or storage device".
/// \return     execution status \ref fsStatus
///               - fsOK               = Operation successful, no allocation errors
///               - fsInvalidDrive     = Nonexistent drive letter specified.
///               - fsAccessDenied     = Unsupported drive.
///               - fsTooManyOpenFiles = File cannot be opened due to too many opened files.
///               - fsError            = Check failed due to allocation errors.
/// \note       This function supports EFS drives only.
extern fsStatus fcheck (const char *drive);

/// \brief Defragment Embedded Flash drive.
/// \param[in]  drive                    a string specifying the \ref drive "memory or storage device".
/// \return     execution status \ref fsStatus
///               - fsOK               = Operation successful.
///               - fsInvalidDrive     = Nonexistent drive letter specified.
///               - fsAccessDenied     = Not all files are closed on specified drive or unsupported drive.
///               - fsTooManyOpenFiles = File cannot be opened due to too many opened files.
///               - fsNoFreeSpace      = Not enough space to complete the defragmentation.
/// \note       This function supports EFS drives only.
extern fsStatus fdefrag (const char *drive);

/// \brief Check if media present on removable drive.
/// \param[in]  drive                    a string specifying the \ref drive "memory or storage device".
/// \return     execution status \ref fsStatus
///               - fsOK               = Operation successful.
///               - fsNoMedia          = Media not present.
///               - fsAccessDenied     = Tried to access non-removable drive.
/// \note       This function supports FAT drives only.
extern fsStatus fmedia (const char *drive);

/// \brief Read drive information.
/// \param[in]  drive                    a string specifying the \ref drive "memory or storage device".
/// \param[out] info                     drive information structure.
/// \return     execution status \ref fsStatus
///               - fsOK               = Operation successful.
///               - fsInvalidParameter = Input parameters invalid.
///               - fsInvalidDrive     = Nonexistent drive letter specified.
extern fsStatus finfo (const char *drive, fsDriveInfo *info);

/// \brief Read volume label and serial number.
/// \param[in]  drive                    a string specifying the \ref drive "memory or storage device".
/// \param[out] label                    buffer for storing label as a null terminated string.
///                                      When volume has no label an empty string is returned.
///                                      This parameter can be NULL meaning label will not be read.
/// \param[out] serial                   pointer to variable where serial number will be stored.
///                                      This parameter can be NULL meaning serial number will not be read.
/// \return     execution status \ref fsStatus
///               - fsOK               = Operation successful.
///               - fsInvalidDrive     = Nonexistent drive letter specified.
///               - fsAccessDenied     = Trying to read label and serial from EFS drive.
///               - fsDriverError      = Read/write error.
/// \note       This function supports FAT drives only.
extern fsStatus fvol (const char *drive, char *label, uint32_t *serial);


//  ==== File Time Support Routines ====

/// \brief Callback function used to provide the current date and time to the File System.
/// \param[out] time                     Pointer to the \ref fsTime structure.
/// \return     execution status \ref fsStatus
///               - fsOK               = Operation successful.
///               - fsError            = Failed to get the current time
/// \note       This function supports FAT drives only.
extern fsStatus fs_get_time (fsTime *time);


//  ==== Media Support Routines ====

/// \brief   Callback function used to read Card Detect (CD) pin state when
///          Memory Card is used in SPI mode.
/// \param[in]  drive_num                Memory Card Drive number
/// \return     1:card detected, 0:card not detected, or error
extern int32_t fs_mc_read_cd (uint32_t drive_num);

/// \brief   Callback function used to read Write Protect (WP) pin state when
///          Memory Card is used in SPI mode.
/// \param[in]  drive_num                Memory Card Drive number
/// \return     1:write protected, 0:not write protected, or error
extern int32_t fs_mc_read_wp (uint32_t drive_num);


//  ==== I/O Control Interface Routines ====

/// \brief Check if valid drive is specified and return its ID.
/// \param[in]  drive                    a string specifying the \ref drive "memory or storage device".
/// \return     drive ID or execution status
///               - value >= 0: drive ID as an integer when specified drive exists and allows IOC access
///               - value < 0: error occurred, -value is execution status as defined with \ref fsStatus
extern int32_t fs_ioc_get_id (const char *drive);

/// \brief Lock drive and block media access to the upper layer.
/// \param[in]  drv_id                   Drive identifier obtained by \ref fs_ioc_get_id.
/// \return     execution status \ref fsStatus
///               - fsOK               = Operation successful.
///               - fsInvalidDrive     = Nonexistent drive id specified.
///               - fsAccessDenied     = IOC access not allowed for specified drive.
extern fsStatus fs_ioc_lock (int32_t drv_id);

/// \brief Unlock drive and allow media access to the upper layer.
/// \param[in]  drv_id                   Drive identifier obtained by \ref fs_ioc_get_id.
/// \return     execution status \ref fsStatus
///               - fsOK               = Operation successful.
///               - fsInvalidDrive     = Nonexistent drive id specified.
///               - fsAccessDenied     = IOC access not allowed for specified drive.
extern fsStatus fs_ioc_unlock (int32_t drv_id);

/// \brief Return IOC cache buffer information.
/// \param[in]  drv_id                   Drive identifier obtained by \ref fs_ioc_get_id.
/// \param[out] cache_info               IOC Cache information.
/// \return     execution status \ref fsStatus
///               - fsOK               = Operation successful.
///               - fsInvalidParameter = Input parameters are not valid.
///               - fsInvalidDrive     = Nonexistent drive id specified.
///               - fsAccessDenied     = IOC access not allowed for specified drive.
extern fsStatus fs_ioc_get_cache (int32_t drv_id, fsIOC_Cache *cache_info);

/// \brief Read sector from media.
/// \param[in]  drv_id                   Drive identifier obtained by \ref fs_ioc_get_id.
/// \param[in]  sect                     Sector number.
/// \param[out] buf                      Data buffer.
/// \param[in]  cnt                      Count of sectors.
/// \return     execution status \ref fsStatus
///               - fsOK               = Operation successful.
///               - fsInvalidDrive     = Nonexistent drive id specified.
///               - fsNoMedia          = Media not present.
///               - fsAccessDenied     = IOC access not allowed for specified drive.
///               - fsError            = Read sector failed.
extern fsStatus fs_ioc_read_sector (int32_t drv_id, uint32_t sect, uint8_t *buf, uint32_t cnt);

/// \brief Write sector to media.
/// \param[in]  drv_id                   Drive identifier obtained by \ref fs_ioc_get_id.
/// \param[in]  sect                     Sector number.
/// \param[out] buf                      Data buffer.
/// \param[in]  cnt                      Count of sectors.
/// \return     execution status \ref fsStatus
///               - fsOK               = Operation successful.
///               - fsInvalidDrive     = Nonexistent drive id specified.
///               - fsNoMedia          = Media not present.
///               - fsAccessDenied     = IOC access not allowed for specified drive.
///               - fsError            = Read sector failed.
extern fsStatus fs_ioc_write_sector (int32_t drv_id, uint32_t sect, const uint8_t *buf, uint32_t cnt);

/// \brief Read media configuration info.
/// \param[in]  drv_id                   Drive identifier obtained by \ref fs_ioc_get_id.
/// \param[out] info                     Media information structure.
/// \return     execution status \ref fsStatus
///               - fsOK               = Operation successful.
///               - fsInvalidDrive     = Nonexistent drive id specified.
///               - fsNoMedia          = Media not present.
///               - fsAccessDenied     = IOC access not allowed for specified drive.
///               - fsError            = Media information read failed.
extern fsStatus fs_ioc_read_info (int32_t drv_id, fsMediaInfo *info);

/// \brief IOC device control access; control code is sent directly to device driver.
/// \param[in]  drv_id                   Drive identifier obtained by \ref fs_ioc_get_id.
/// \param[in]  code                     Device control code.
/// \param[in,out] p                     Generic pointer.
/// \return     execution status \ref fsStatus
extern fsStatus fs_ioc_device_ctrl (int32_t drv_id, fsDevCtrlCode code, void *p);

#ifdef __cplusplus
}
#endif

#endif /* __RL_FS_H */
