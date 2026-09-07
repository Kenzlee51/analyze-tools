/*------------------------------------------------------------------------------
 * MDK Middleware - Component ::USB
 * Copyright (c) 2004-2014 ARM Germany GmbH. All rights reserved.
 *------------------------------------------------------------------------------
 * Name:    rl_usb.h
 * Purpose: USB User API
 * Rev.:    V6.2
 *----------------------------------------------------------------------------*/

#ifndef __RL_USB_H__
#define __RL_USB_H__


#ifdef __cplusplus
extern "C"  {
#endif

#include <stdint.h>
#include <stdbool.h>
#include "cmsis_os.h"
#include "usb_def.h"
#include "usb_cdc.h"
#include "usb_hid.h"
#include "usb_msc.h"


//  ==== USB Constants and Defines ====

/// Status code values returned by USB library functions.
typedef enum {
  usbOK                       =    0,   ///< Function completed with no error

  usbTimeout,                           ///< Function completed; time-out occurred
  usbInvalidParameter,                  ///< Invalid Parameter error: a mandatory parameter was missing or specified an incorrect object

  usbThreadError              = 0x10,   ///< CMSIS-RTOS Thread creation/termination failed
  usbTimerError,                        ///< CMSIS-RTOS Timer creation/deletion failed
  usbSemaphoreError,                    ///< CMSIS-RTOS Semaphore creation failed

  usbControllerError          = 0x20,   ///< Controller does not exist
  usbDeviceError,                       ///< Device does not exist
  usbDriverError,                       ///< Driver function produced error
  usbDriverBusy,                        ///< Driver function is busy
  usbMemoryError,                       ///< Memory management function produced error
  usbNotConfigured,                     ///< Device is not configured (is connected)
  usbClassErrorADC,                     ///< Audio Device Class (ADC) error (no device or device produced error)
  usbClassErrorCDC,                     ///< Communication Device Class (CDC) error (no device or device produced error)
  usbClassErrorHID,                     ///< Human Interface Device (HID) error (no device or device produced error)
  usbClassErrorMSC,                     ///< Mass Storage Device (MSC) error (no device or device produced error)
  usbClassErrorCustom,                  ///< Custom device Class (Class) error (no device or device produced error)
  usbUnsupportedClass,                  ///< Unsupported Class

  usbUnknownError             = 0xFF    ///< Unspecified USB error
} usbStatus;

/// Endianess handling macros
#ifdef __BIG_ENDIAN
 #define U32_LE(v)  (uint32_t)(__rev(v))
 #define U16_LE(v)  (uint16_t)(__rev(v) >> 16)
 #define U32_BE(v)  (uint32_t)(v)
 #define U16_BE(v)  (uint16_t)(v)
#else
 #define U32_BE(v)  (uint32_t)(__rev(v))
 #define U16_BE(v)  (uint16_t)(__rev(v) >> 16)
 #define U32_LE(v)  (uint32_t)(v)
 #define U16_LE(v)  (uint16_t)(v)
#endif

//  ==== USB Device Constants and Defines ====

/// USB Device Custom Class API enumerated constants
typedef enum {
  usbdRequestNotProcessed = 0,          ///< Request not processed
  usbdRequestOK,                        ///< Request processed and OK
  usbdRequestStall,                     ///< Request processed but unsupported
  usbdRequestNAK                        ///< Request processed but busy
} usbdRequestStatus;

/// USB Device HID Class API enumerated constants
typedef enum {
  USBD_HID_REQ_EP_CTRL = 0,             ///< Request from control endpoint
  USBD_HID_REQ_EP_INT,                  ///< Request from interrupt endpoint
  USBD_HID_REQ_PERIOD_UPDATE            ///< Request from periodic update
} USBD_HID_REQ_t;

/// USB Device Mass Storage data structure (containing runtime values for MSC device instance)
typedef struct _usbd_msc_data_t {
  MSC_CBW           cbw;                ///< command block wrapper
  MSC_CSW           csw;                ///< command status wrapper
  uint32_t          block;              ///< read/write operation block
  uint32_t          offset;             ///< read/write operation offset
  uint32_t          length;             ///< read write operation remaining length
  bool              mem_ok;             ///< memory verify status
  uint8_t           bulk_stage;         ///< bulk stage
  uint32_t          bulk_len;           ///< bulk in/out length
  uint32_t          bulk_req_len;       ///< bulk in/out requested length
  bool              media_ready;        ///< media ready flag
  bool              read_only;          ///< media read only flag
  uint32_t          memory_size;        ///< media memory size
  uint32_t          block_size;         ///< media block size
  uint32_t          block_group;        ///< blocks available size in cache
  uint32_t          block_count;        ///< media total number of blocks
  uint8_t          *block_buf;          ///< data buffer for media data read/write
  bool              media_ready_ex;     ///< previous state of media ready flag
} usbd_msc_data_t;

/// USB Device Mass Storage configuration structure (containing configuration values for MSC device instance)
/// @cond usbd_msc_t_cond
typedef struct _usbd_msc_t {
  uint8_t          *bulk_buf;           ///< data buffer for bulk transfers
  usbd_msc_data_t  *data_ptr;           ///< pointer to structure containing runtime values
  uint8_t           dev_num;            ///< device instance configuration setting
  uint8_t           if_num;             ///< interface number
  uint8_t           ep_bulk_in;         ///< bulk in endpoint number
  uint8_t           ep_bulk_out;        ///< bulk out endpoint number
  uint32_t          bulk_buf_sz;        ///< size of bulk buffer
  uint16_t          max_packet_size[2]; ///< maximum packet size for bulk endpoints (for LS/FS and HS)
  uint8_t          *inquiry_data;       ///< data returned upon SCSI Inquiry request
} const usbd_msc_t;
/// @endcond

//  ==== USB Host Constants and Defines ====

/// USB Host Pipe settings structure
typedef struct {
  uint32_t          hw_handle;          ///< Handle to Hardware resource
  uint8_t           dev_addr;           ///< Device communication Address
  uint8_t           dev_speed;          ///< Device communication Speed
  uint8_t           hub_addr;           ///< Hub communication Address
  uint8_t           hub_port;           ///< Hub communication Port
  uint8_t           bEndpointAddress;   ///< Endpoint Address + direction
  uint8_t           bmAttributes;       ///< Endpoint Attributes (type + Isochronous info)
  uint16_t          wMaxPacketSize;     ///< Maximum Packet Size + Isochronous info
  uint8_t           bInterval;          ///< Interval
  uint32_t          transferred;        ///< Last transferred information
  uint8_t           active;             ///< Activity flag
} USBH_PIPE;

/// USB Host Device Instance (DEV) structure
typedef struct {
  uint8_t           ctrl;               ///< Index of USB Host controller
  uint8_t           dev_addr;           ///< Device communication Address
  uint8_t           dev_speed;          ///< Device communication Speed
  uint8_t           hub_addr;           ///< Hub communication Address
  uint8_t           hub_port;           ///< Hub communication Port
  struct {
    uint8_t         configured  : 1;    ///< Device Configured status
    uint8_t         initialized : 1;    ///< Device Initialized status
  } state;
  uint8_t           max_packet_size;    ///< Maximum Packet Size
  uint8_t           vid;                ///< Vendor ID
  uint8_t           pid;                ///< Product ID

  uint8_t           class_custom;       ///< Class Custom handling
  uint8_t           class_instance;     ///< Class instance
  uint8_t           class_driver;       ///< Class Driver used
  uint8_t           dev_desc_len;       ///< Device Descriptor Length
  uint8_t           cfg_desc_len;       ///< Configuration Descriptor Length
  osThreadId        recovery_thread_id; ///< Thread ID of thread that activated Recovery
} USBH_DEV;


//  ==== USB Host externally defined variables ====

/// USB Host number of Custom Class (CLS) instances as defined in USBH_Config_Class.h file
extern const uint8_t usbh_cls_num;


//  ==== USB Device Functions ====

/// \brief Initialize USB Device stack and controller
/// \param[in]     device               index of USB Device.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBD_Initialize (uint8_t device);

/// \brief De-initialize USB Device stack and controller
/// \param[in]     device               index of USB Device.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBD_Uninitialize (uint8_t device);

/// \brief Activate pull-up on D+ or D- line to signal USB Device connection on USB Bus
/// \param[in]     device               index of USB Device.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBD_Connect (uint8_t device);

/// \brief Disconnect USB Device from USB Bus
/// \param[in]     device               index of USB Device.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBD_Disconnect (uint8_t device);

/// \brief Return USB Device configuration status
/// \param[in]     device               index of USB Device.
/// \return        true                 device is in configured state and ready to communicate.
/// \return        false                device is not configured and not ready to communicate.
extern bool USBD_Configured (uint8_t device);


//  ==== USB Device Audio Device Functions ====

#ifdef __DOXYGEN__

// following functions are available for each instance of a ADC class.
// generic prefix USBD_ADCn is USBD_ADC0 for ADC class instance 0.

/// \brief Called during \ref USBD_Initialize to initialize the USB ADC class Device
/// \return                             none.
void USBD_ADCn_Initialize (void);

/// \brief Called during \ref USBD_Uninitialize to de-initialize the USB ADC class Device
/// \return                             none.
void USBD_ADCn_Uninitialize (void);

/// \brief Callback function called when speaker activity (interface) setting changed event
/// \param[in]     active               activity status.
/// \return                             none.
void USBD_ADCn_SpeakerStatusEvent (bool active);

/// \brief Callback function called when speaker mute setting changed event
/// \param[in]     ch                   channel index.
///                                       - value 0: master channel
///                                       - value 1: left speaker (in stereo mode)
///                                       - value 2: right speaker (in stereo mode)
/// \param[in]     cur                  current setting.
/// \return                             none.
void USBD_ADCn_SpeakerMuteEvent (uint8_t ch, bool cur);

/// \brief Callback function called when speaker volume setting changed event
/// \param[in]     ch                   channel index.
///                                       - value 0: master channel
///                                       - value 1: left speaker (in stereo mode)
///                                       - value 2: right speaker (in stereo mode)
/// \param[in]     cur                  current setting.
/// \return                             none.
void USBD_ADCn_SpeakerVolumeEvent (uint8_t ch, uint16_t cur);

/// \brief Callback function called when microphone activity (interface) setting changed event
/// \param[in]     active               activity status.
/// \return                             none.
void USBD_ADCn_MicrophoneStatusEvent (bool active);

/// \brief Callback function called when microphone mute setting changed event
/// \param[in]     ch                   channel index.
///                                       - value 0: master channel
///                                       - value 1: left microphone (in stereo mode)
///                                       - value 2: right microphone (in stereo mode)
/// \param[in]     cur                  current setting.
/// \return                             none.
void USBD_ADCn_MicrophoneMuteEvent (uint8_t ch, bool cur);

/// \brief Callback function called when microphone volume setting changed event
/// \param[in]     ch                   channel index.
///                                       - value 0: master channel
///                                       - value 1: left microphone (in stereo mode)
///                                       - value 2: right microphone (in stereo mode)
/// \param[in]     cur                  current setting.
/// \return                             none.
void USBD_ADCn_MicrophoneVolumeEvent (uint8_t ch, uint16_t cur);

#endif // __DOXYGEN

/// \brief Set range for speaker volume control
/// \param[in]     instance             instance of ADC class.
/// \param[in]     ch                   channel index.
///                                       - value 0: master channel
///                                       - value 1: left speaker (in stereo mode)
///                                       - value 2: right speaker (in stereo mode)
/// \param[in]     min                  minimum volume value.
/// \param[in]     max                  maximum volume value.
/// \param[in]     res                  volume resolution.
/// \param[in]     cur                  current volume value.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBD_ADC_SpeakerSetVolumeRange (uint8_t instance, uint8_t ch, uint16_t min, uint16_t max, uint16_t res, uint16_t cur);

/// \brief Set range for microphone volume (level) control
/// \param[in]     instance             instance of ADC class.
/// \param[in]     ch                   channel index.
///                                       - value 0: master channel
///                                       - value 1: left microphone (in stereo mode)
///                                       - value 2: right microphone (in stereo mode)
/// \param[in]     min                  minimum volume value.
/// \param[in]     max                  maximum volume value.
/// \param[in]     res                  volume resolution.
/// \param[in]     cur                  current volume value.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBD_ADC_MicrophoneSetVolumeRange (uint8_t instance, uint8_t ch, uint16_t min, uint16_t max, uint16_t res, uint16_t cur);

/// \brief Number of audio samples received from USB Host and available to be read
/// \param[in]     instance             instance of ADC class.
/// \return                             number of samples available to be read.
extern uint32_t USBD_ADC_ReceivedSamplesAvailable (uint8_t instance);

/// \brief Number of audio samples written and pending to be sent to USB Host
/// \param[in]     instance             instance of ADC class.
/// \return                             number of samples ready to be sent.
extern uint32_t USBD_ADC_WrittenSamplesPending (uint8_t instance);

/// \brief Read audio samples received from USB Host
/// \param[in]     instance             instance of ADC class.
/// \param[out]    buf                  buffer that receives samples.
/// \param[in]     num                  maximum number of samples to read.
/// \return                             number of samples read or execution status.
///                - value >= 0:        number of samples read
///                - value < 0:         error occurred, -value is execution status as defined with \ref usbStatus
extern int32_t USBD_ADC_ReadSamples (uint8_t instance, void *buf, int32_t num);

/// \brief Write audio samples to be transferred to USB Host
/// \param[in]     instance             instance of ADC class.
/// \param[in]     buf                  buffer containing samples to write.
/// \param[in]     num                  maximum number of samples to write.
/// \return                             number of samples written or execution status.
///                - value >= 0:        number of samples written for sending
///                - value < 0:         error occurred, -value is execution status as defined with \ref usbStatus
extern int32_t USBD_ADC_WriteSamples (uint8_t instance, const void *buf, int32_t num);


//  ==== USB Device Communication Device (Abstract Control Model) Functions ====

#ifdef __DOXYGEN__

// following functions are available for each instance of a CDC class.
// generic prefix USBD_CDCn is USBD_CDC0 for CDC class instance 0.

/// \brief Called during \ref USBD_Initialize to initialize the USB CDC class Device (Virtual COM Port)
/// \return                             none.
void USBD_CDCn_ACM_Initialize (void);

/// \brief Called during \ref USBD_Uninitialize to de-initialize the USB CDC class Device (Virtual COM Port)
/// \return                             none.
void USBD_CDCn_ACM_Uninitialize (void);

/// \brief Called during USB Bus reset to reset the USB CDC class Device (Virtual COM Port)
/// \return                             none.
void USBD_CDCn_ACM_Reset (void);

/// \brief Change communication settings of USB CDC class Device (Virtual COM Port)
/// \param[in]     line_coding          pointer to CDC_LINE_CODING structure.
/// \return        true                 set line coding request processed.
/// \return        false                set line coding request not supported or not processed.
bool USBD_CDCn_ACM_SetLineCoding (CDC_LINE_CODING *line_coding);

/// \brief Retrieve communication settings of USB CDC class Device (Virtual COM Port)
/// \param[out]    line_coding          pointer to CDC_LINE_CODING structure.
/// \return        true                 get line coding request processed.
/// \return        false                get line coding request not supported or not processed.
bool USBD_CDCn_ACM_GetLineCoding (CDC_LINE_CODING *line_coding);

/// \brief Set control line states of USB CDC class Device (Virtual COM Port)
/// \param[in]     state                control line settings bitmap.
///                                       - bit 0: DTR state
///                                       - bit 1: RTS state
/// \return        true                 set control line state request processed.
/// \return        false                set control line state request not supported or not processed.
bool USBD_CDCn_ACM_SetControlLineState (uint16_t state);

/// \brief Function indicating new data was received by USB CDC class Device (Virtual COM Port)
/// \param[in]     len                  number of bytes available to read.
/// \return                             none.
void USBD_CDCn_ACM_DataReceived (uint32_t len);

/// \brief Function indicating all data was sent by USB CDC class Device (Virtual COM Port)
/// \return                             none.
void USBD_CDCn_ACM_DataSent (void);

#endif // __DOXYGEN

/// \brief Read one character received by Communication Device from USB Host
/// \param[in]     instance             instance of CDC class.
/// \return                             value of read character or no character received.
///                - value >= 0:        value of first received unread character
///                - value -1:          indicates no character was received
extern int USBD_CDC_ACM_GetChar (uint8_t instance);

/// \brief Write a single character from Communication Device to USB Host
/// \param[in]     instance             instance of CDC class.
/// \param[in]     ch                   character to write.
/// \return                             value of accepted character or no character accepted.
///                - value ch:          if character accepted for writing
///                - value -1:          indicates character not accepted
extern int USBD_CDC_ACM_PutChar (uint8_t instance, int ch);

/// \brief Read multiple data bytes received by Communication Device from USB Host
/// \param[in]     instance             instance of CDC class.
/// \param[out]    buf                  buffer that receives data.
/// \param[in]     len                  maximum number of bytes to read.
/// \return                             number of bytes read or execution status.
///                - value >= 0:        number of bytes read
///                - value < 0:         error occurred, -value is execution status as defined with \ref usbStatus
extern int32_t USBD_CDC_ACM_ReadData (uint8_t instance, uint8_t *buf, int32_t len);

/// \brief Write data from Communication Device to USB Host
/// \param[in]     instance             instance of CDC class.
/// \param[in]     buf                  buffer containing data bytes to write.
/// \param[in]     len                  maximum number of bytes to write.
/// \return                             number of bytes accepted for writing or execution status.
///                - value >= 0:        number of bytes accepted for writing
///                - value < 0:         error occurred, -value is execution status as defined with \ref usbStatus
extern int32_t USBD_CDC_ACM_WriteData (uint8_t instance, const uint8_t *buf, int32_t len);

/// \brief Retrieve number of data bytes received by Communication Device from
///        USB Host that are available to read
/// \param[in]     instance             instance of CDC class.
/// \return                             number of bytes available to read.
extern int32_t USBD_CDC_ACM_DataAvailable (uint8_t instance);

/// \brief Send notification of Communication Device status and line states to USB Host
/// \param[in]     instance             instance of CDC class.
/// \param[in]     state                error status and line states:
///                                       - bit 6: bOverRun
///                                       - bit 5: bParity
///                                       - bit 4: bFraming
///                                       - bit 3: bRingSignal
///                                       - bit 2: bBreak
///                                       - bit 1: bTxCarrier (DSR line state)
///                                       - bit 0: bRxCarrier (DCD line state)
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBD_CDC_ACM_Notify (uint8_t instance, uint16_t state);


//  ==== USB Device Human Interface Device Functions ====

#ifdef __DOXYGEN__

// following functions are available for each instance of a HID class.
// generic prefix USBD_HIDn is USBD_HID0 for HID class instance 0.

/// \brief Called during \ref USBD_Initialize to initialize the USB HID class Device
/// \return                             none.
void USBD_HIDn_Initialize (void);

/// \brief Called during \ref USBD_Uninitialize to de-initialize the USB HID class Device
/// \return                             none.
void USBD_HIDn_Uninitialize (void);

/// \brief Prepare HID Report data to send
/// \param[in]     rtype                report type:
///                  - HID_REPORT_INPUT           = input report requested
///                  - HID_REPORT_FEATURE         = feature report requested
/// \param[in]     req                  request type:
///                  - USBD_HID_REQ_EP_CTRL       = control endpoint request
///                  - USBD_HID_REQ_PERIOD_UPDATE = idle period expiration request
///                  - USBD_HID_REQ_EP_INT        = previously sent report on interrupt endpoint request
/// \param[in]     rid                  report ID (0 if only one report exists).
/// \param[out]    buf                  buffer containing report data to send.
/// \return                             number of report data bytes prepared to send or invalid report requested.
///                - value >= 0:        number of report data bytes prepared to send
///                - value = -1:        invalid report requested
int32_t USBD_HIDn_GetReport (uint8_t rtype, uint8_t req, uint8_t rid, uint8_t *buf);

/// \brief Process received HID Report data
/// \param[in]     rtype                report type:
///                  - HID_REPORT_OUTPUT    = output report received
///                  - HID_REPORT_FEATURE   = feature report received
/// \param[in]     req                  request type:
///                  - USBD_HID_REQ_EP_CTRL = report received on control endpoint
///                  - USBD_HID_REQ_EP_INT  = report received on interrupt endpoint
/// \param[in]     rid                  report ID (0 if only one report exists).
/// \param[in]     buf                  buffer that receives report data.
/// \param[in]     len                  length of received report data.
/// \return        true                 received report data processed.
/// \return        false                received report data not processed or request not supported.
bool USBD_HIDn_SetReport (uint8_t rtype, uint8_t req, uint8_t rid, const uint8_t *buf, int32_t len);

#endif // __DOXYGEN

/// \brief Asynchronously prepare HID Report data to send
/// \param[in]     instance             instance of HID class.
/// \param[in]     rid                  report ID.
/// \param[out]    buf                  buffer containing report data to send.
/// \param[in]     len                  number of report data bytes to send.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBD_HID_GetReportTrigger (uint8_t instance, uint8_t rid, const uint8_t *buf, int32_t len);


#ifdef __DOXYGEN__

//  ==== USB Device Mass Storage Device Functions ====

// following functions are available for each instance of a MSC class.
// generic prefix USBD_MSCn is USBD_MSC0 for MSC class instance 0.

/// \brief Called during \ref USBD_Initialize to initialize the USB MSC class Device
/// \return                             none.
void USBD_MSCn_Initialize (void);

/// \brief Called during \ref USBD_Uninitialize to de-initialize the USB MSC class Device
/// \return                             none.
void USBD_MSCn_Uninitialize (void);

/// \brief Read data from media
/// \param[in]     lba                  logical address of first block to read.
/// \param[in]     cnt                  number of contiguous blocks to read from media.
/// \param[out]    buf                  data buffer for data read from media.
/// \return        true                 read succeeded.
/// \return        false                read failed.
bool USBD_MSCn_Read (uint32_t lba, uint32_t cnt, uint8_t *buf);

/// \brief Write data to media
/// \param[in]     lba                  logical address of first block to write.
/// \param[in]     cnt                  number of contiguous blocks to write to media.
/// \param[out]    buf                  data buffer containing data to write to media.
/// \return        true                 write succeeded.
/// \return        false                write failed.
bool USBD_MSCn_Write (uint32_t lba, uint32_t cnt, const uint8_t *buf);

/// \brief Check if media present
/// \return        true                 media is present.
/// \return        false                media is not present.
bool USBD_MSCn_CheckMedia (void);

#endif // __DOXYGEN


//  ==== USB Device Custom Class Functions ====

#ifdef __DOXYGEN__

// following functions are available for each instance of a Custom class.
// generic prefix USBD_CustomClassn is USBD_CustomClassn for Custom class instance 0.

/// \brief Called during \ref USBD_Initialize to initialize the USB Custom class Device
/// \return                             none.
void USBD_CustomClassn_Initialize (void);

/// \brief Called during \ref USBD_Uninitialize to de-initialize the USB Custom class Device
/// \return                             none.
void USBD_CustomClassn_Uninitialize (void);

/// \brief Custom Class Reset Event handling
/// \return                             none.
void USBD_CustomClassn_EventReset (void);

/// \brief Custom Class Endpoint Start Event handling
/// \param[in]     ep_addr              endpoint address.
///                 - ep_addr.0..3:       address
///                 - ep_addr.7:          direction
/// \return                             none.
void USBD_CustomClassn_EventEndpointStart (uint8_t ep_addr);

/// \brief Custom Class Endpoint Stop Event handling
/// \param[in]     ep_addr              endpoint address.
///                 - ep_addr.0..3:       address
///                 - ep_addr.7:          direction
/// \return                             none.
void USBD_CustomClassn_EventEndpointStop (uint8_t ep_addr);

/// \brief Callback function called when a SETUP PACKET was received on Control Endpoint 0
/// \param[in]     setup_packet         pointer to received setup packet.
/// \param[out]    buf                  pointer to data buffer used for data stage requested by setup packet.
/// \param[out]    len                  pointer to number of data bytes in data stage requested by setup packet.
/// \return        usbdRequestStatus    enumerator value indicating the function execution status
/// \return        usbdRequestNotProcessed:request was not processed; processing will be done by USB library
/// \return        usbdRequestOK:       request was processed successfully (send Zero-Length Packet if no data stage)
/// \return        usbdRequestStall:    request was processed but is not supported (STALL EP)
usbdRequestStatus USBD_CustomClassn_Endpoint0_SetupPacketReceived (const USB_SETUP_PACKET *setup_packet, uint8_t **buf, uint32_t *len);

/// \brief Callback function called when a SETUP PACKET was processed by USB library
/// \param[in]     setup_packet         pointer to processed setup packet.
/// \return                             none.
void USBD_CustomClassn_Endpoint0_SetupPacketProcessed (const USB_SETUP_PACKET *setup_packet);

/// \brief Callback function called when OUT DATA was received on Control Endpoint 0
/// \param[in]     len                  number of received data bytes.
/// \return        usbdRequestStatus    enumerator value indicating the function execution status
/// \return        usbdRequestNotProcessed:request was not processed; processing will be done by USB library
/// \return        usbdRequestOK:       request was processed successfully (send Zero-Length Packet)
/// \return        usbdRequestStall:    request was processed but is not supported (stall endpoint 0)
/// \return        usbdRequestNAK:      request was processed but the device is busy (return NAK)
usbdRequestStatus USBD_CustomClassn_Endpoint0_OutDataReceived (uint32_t len);

/// \brief Callback function called when IN DATA was sent on Control Endpoint 0
/// \param[in]     len                  number of sent data bytes.
/// \return        usbdRequestStatus    enumerator value indicating the function execution status
/// \return        usbdRequestNotProcessed:request was not processed; processing will be done by USB library
/// \return        usbdRequestOK:       request was processed successfully (return ACK)
/// \return        usbdRequestStall:    request was processed but is not supported (stall endpoint 0)
/// \return        usbdRequestNAK:      request was processed but the device is busy (return NAK)
usbdRequestStatus USBD_CustomClassn_Endpoint0_InDataSent (uint32_t len);

/// \brief Callback function called when DATA was sent or received on Endpoint n
/// \param[in]     event                event on Endpoint:
///                                       - ARM_USBD_EVENT_OUT = data OUT received
///                                       - ARM_USBD_EVENT_IN  = data IN  sent
void USBD_CustomClassn_Endpoint1_Event  (uint32_t event);
void USBD_CustomClassn_Endpoint2_Event  (uint32_t event);
void USBD_CustomClassn_Endpoint3_Event  (uint32_t event);
void USBD_CustomClassn_Endpoint4_Event  (uint32_t event);
void USBD_CustomClassn_Endpoint5_Event  (uint32_t event);
void USBD_CustomClassn_Endpoint6_Event  (uint32_t event);
void USBD_CustomClassn_Endpoint7_Event  (uint32_t event);
void USBD_CustomClassn_Endpoint8_Event  (uint32_t event);
void USBD_CustomClassn_Endpoint9_Event  (uint32_t event);
void USBD_CustomClassn_Endpoint10_Event (uint32_t event);
void USBD_CustomClassn_Endpoint11_Event (uint32_t event);
void USBD_CustomClassn_Endpoint12_Event (uint32_t event);
void USBD_CustomClassn_Endpoint13_Event (uint32_t event);
void USBD_CustomClassn_Endpoint14_Event (uint32_t event);
void USBD_CustomClassn_Endpoint15_Event (uint32_t event);

#endif // __DOXYGEN

/// \brief Start reception on Endpoint
/// \param[in]     device               index of USB Device.
/// \param[in]     ep_addr              endpoint address
///                 - ep_addr.0..3:       address
///                 - ep_addr.7:          direction
/// \param[out]    buf                  buffer that receives data.
/// \param[in]     len                  maximum number of bytes to receive.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBD_EndpointRead (uint8_t device, uint8_t ep_addr, uint8_t *buf, uint32_t len);

/// \brief Get result of read operation on Endpoint
/// \param[in]     device               index of USB Device.
/// \param[in]     ep_addr              endpoint address
///                 - ep_addr.0..3:       address
///                 - ep_addr.7:          direction
/// \return                             number of bytes received.
extern uint32_t USBD_EndpointReadGetResult (uint8_t device, uint8_t ep_addr);

/// \brief Start write on Endpoint
/// \param[in]     device               index of USB Device.
/// \param[in]     ep_addr              endpoint address
///                 - ep_addr.0..3:       address
///                 - ep_addr.7:          direction
/// \param[out]    buf                  buffer containing data bytes to write.
/// \param[in]     len                  maximum number of bytes to write.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBD_EndpointWrite (uint8_t device, uint8_t ep_addr, const uint8_t *buf, uint32_t len);

/// \brief Get result of write operation on Endpoint
/// \param[in]     device               index of USB Device.
/// \param[in]     ep_addr              endpoint address
///                 - ep_addr.0..3:       address
///                 - ep_addr.7:          direction
/// \return                             number of bytes written.
extern uint32_t USBD_EndpointWriteGetResult (uint8_t device, uint8_t ep_addr);

/// \brief Set/Clear stall on Endpoint
/// \param[in]     device               index of USB Device.
/// \param[in]     ep_addr              endpoint address.
///                 - ep_addr.0..3:       address
///                 - ep_addr.7:          direction
/// \param[in]     stall                false = Clear stall, true = Set stall.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBD_EndpointStall (uint8_t device, uint8_t ep_addr, bool stall);

/// \brief Abort read/write operation on Endpoint
/// \param[in]     device               index of USB Device.
/// \param[in]     ep_addr              endpoint address
///                 - ep_addr.0..3:       address
///                 - ep_addr.7:          direction
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBD_EndpointAbort (uint8_t device, uint8_t ep_addr);


//  ==== USB Host Functions ====

/// \brief Initialize USB Host stack and controller
/// \param[in]     ctrl                 index of USB Host controller.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_Initialize (uint8_t ctrl);

/// \brief De-initialize USB Host stack and controller
/// \param[in]     ctrl                 index of USB Host controller.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_Uninitialize (uint8_t ctrl);

/// \brief Get status of USB Device
/// \param[in]     device               index of USB Device.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_GetDeviceStatus (uint8_t device);


//  ==== USB Host Mass Storage Functions ====

/// \brief Get status of Mass Storage Device
/// \param[in]     instance             instance of MSC Device.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_MSC_GetDeviceStatus (uint8_t instance);

/// \brief Read requested number of blocks from Mass Storage Device
/// \param[in]     instance             instance of MSC Device.
/// \param[in]     lba                  logical block address of first block to read.
/// \param[in]     cnt                  number of contiguous blocks to read.
/// \param[out]    buf                  data buffer in which to read data.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_MSC_Read (uint8_t instance, uint32_t lba, uint32_t cnt, uint8_t *buf);

/// \brief Write requested number of blocks to Mass Storage Device
/// \param[in]     instance             instance of MSC Device.
/// \param[in]     lba                  logical address of first block to write.
/// \param[in]     cnt                  number of contiguous blocks to write.
/// \param[in]     buf                  data buffer containing data to write.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_MSC_Write (uint8_t instance, uint32_t lba, uint32_t cnt, const uint8_t *buf);

/// \brief Read capacity of Mass Storage Device
/// \param[in]     instance             instance of MSC Device.
/// \param[out]    block_count          pointer to where total number of blocks available will be read.
/// \param[out]    block_size           pointer to where block size will be read.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_MSC_ReadCapacity (uint8_t instance, uint32_t *block_count, uint32_t *block_size);


//  ==== USB Host Human Interface Device Functions ====

/// \brief Get status of Human Interface Device
/// \param[in]     instance             instance of HID Device.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_HID_GetDeviceStatus (uint8_t instance);

/// \brief Read data received from Human Interface Device
/// \param[in]     instance             instance of HID Device.
/// \param[out]    buf                  buffer that receives data.
/// \param[in]     len                  maximum number of bytes to read.
/// \return                             number of bytes read or execution status.
///                - value >= 0:        number of bytes read
///                - value < 0:         error occurred, -value is execution status as defined with \ref usbStatus
extern int32_t USBH_HID_Read (uint8_t instance, uint8_t *buf, int32_t len);

/// \brief Write data to Human Interface Device
/// \param[in]     instance             instance of HID Device.
/// \param[in]     buf                  data buffer containing data to write.
/// \param[in]     len                  number of data bytes to write.
/// \return                             number of bytes accepted for writing or execution status.
///                - value >= 0:        number of bytes accepted for writing
///                - value < 0:         error occurred, -value is execution status as defined with \ref usbStatus
extern int32_t USBH_HID_Write (uint8_t instance, const uint8_t *buf, int32_t len);

/// \brief Retrieve first pending pressed keyboard key on HID Keyboard
/// \param[in]     instance             instance of HID Device.
/// \return                             value of read character or no character received.
///                - value >= 0:        value of first received unread character
///                - value -1:          indicates no character was received
extern int USBH_HID_GetKeyboardKey (uint8_t instance);

/// Mouse state information.
typedef struct _usbHID_MouseState {
  uint8_t button;                       ///< Current button states
  int16_t x;                            ///< Absolute X position change
  int16_t y;                            ///< Absolute Y position change
} usbHID_MouseState;

/// \brief Retrieve state change since last call of this function
/// \param[in]     instance             instance of HID Device.
/// \param[out]    state                pointer to mouse state \ref usbHID_MouseState structure.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_HID_GetMouseState (uint8_t instance, usbHID_MouseState *state);

/// \brief Callback function called for parsing of the Human Interface Device report descriptor
/// \param[in]     instance             instance index.
/// \param[in]     ptr_hid_report_desc  pointer to HID report descriptor.
/// \return                             none.
extern void USBH_HID_ParseReportDescriptor (uint8_t instance, uint8_t *ptr_hid_report_desc);

/// \brief Callback function called when data is received from the Human Interface Device
/// \param[in]     instance             instance index.
/// \param[in]     len                  length of received data.
/// \return                             none.
extern void USBH_HID_DataReceived (uint8_t instance, uint32_t len);


//  ==== USB Host Communication Device Class (Abstract Control Model) Functions ====

/// \brief Get status of Communication Device Class device
/// \param[in]     instance             instance of CDC Device.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_CDC_ACM_GetDeviceStatus (uint8_t instance);

/// \brief Send data to Communication Device Class device
/// \param[in]     instance             index of CDC instance.
/// \param[in]     data                 buffer containing data bytes to send.
/// \param[in]     num                  number of bytes to send.
/// \return                             status code that indicates the execution status of the function as defined with usbStatus.
extern usbStatus USBH_CDC_ACM_Send (uint8_t instance, const uint8_t *data, uint32_t num);

/// \brief Get result of send data to Communication Device Class device
/// \param[in]     instance             index of CDC instance.
/// \return                             number of successfully sent data bytes.
extern uint32_t USBH_CDC_ACM_GetTxCount (uint8_t instance);

/// \brief Receive data from Communication Device Class device
/// \param[in]     instance             index of CDC instance
/// \param[out]    data                 buffer that receives data.
/// \param[in]     num                  maximum number of bytes to receive.
/// \return                             status code that indicates the execution status of the function as defined with usbStatus.
extern usbStatus USBH_CDC_ACM_Receive (uint8_t instance, uint8_t *data, uint32_t num);

/// \brief Get result of receive data from Communication Device Class device
/// \param[in]     instance             index of CDC instance.
/// \return                             number of successfully received data bytes.
extern uint32_t USBH_CDC_ACM_GetRxCount (uint8_t instance);

/// \brief Change communication settings of Communication Device Class device
/// \param[in]     instance             index of CDC instance.
/// \param[in]     line_coding          pointer to CDC_LINE_CODING structure.
/// \return                             status code that indicates the execution status of the function as defined with usbStatus.
extern usbStatus USBH_CDC_ACM_SetLineCoding (uint8_t instance, CDC_LINE_CODING *line_coding);

/// \brief Retrieve communication settings of Communication Device Class device
/// \param[in]     instance             index of CDC instance.
/// \param[out]    line_coding          pointer to CDC_LINE_CODING structure.
/// \return                             status code that indicates the execution status of the function as defined with usbStatus.
extern usbStatus USBH_CDC_ACM_GetLineCoding (uint8_t instance, CDC_LINE_CODING *line_coding);

/// \brief Set control line states of Communication Device Class device
/// \param[in]     instance             index of CDC instance.
/// \param[in]     state                control line settings bitmap.
///                                       - bit 0: DTR state
///                                       - bit 1: RTS state
/// \return                             status code that indicates the execution status of the function as defined with usbStatus.
extern usbStatus USBH_CDC_ACM_SetControlLineState (uint8_t instance, uint16_t state);

/// \brief Callback function called when Communication Device Class device
///        modem line or error status changes
/// \param[in]     instance             index of CDC instance.
/// \param[in]     status               error status and line states:
///                                       - bit 6: bOverRun
///                                       - bit 5: bParity
///                                       - bit 4: bFraming
///                                       - bit 3: bRingSignal
///                                       - bit 2: bBreak
///                                       - bit 1: bTxCarrier (DSR line state)
///                                       - bit 0: bRxCarrier (DCD line state)
/// \return                             none.
extern void USBH_CDC_ACM_Notify (uint8_t instance, uint16_t status);

/// \brief Send break on Communication Device Class device
/// \param[in]     instance             index of CDC instance.
/// \param[in]     duration             duration of break (in milliseconds)
///                                       - value 0xFFFF: indefinite
///                                       - value      0: immediate
/// \return                             status code that indicates the execution status of the function as defined with usbStatus.
extern usbStatus USBH_CDC_ACM_SendBreak (uint8_t instance, uint16_t duration);


//  ==== USB Host Custom Class Functions ====

/// \brief Get status of Custom Class Device
/// \param[in]     instance             instance of Custom Class Device.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_CustomClass_GetDeviceStatus (uint8_t instance);

/// \brief Callback function called when custom class device is connected and needs
///        to configure resources used by custom class device instance
/// \param[in]     ptr_dev              pointer to device structure.
/// \param[in]     ptr_dev_desc         pointer to device descriptor.
/// \param[in]     ptr_cfg_desc         pointer to configuration descriptor.
/// \return        value <= 127         index of configured custom class device instance.
/// \return        value == 0xFF        configuration failed.
extern uint8_t USBH_CustomClass_Configure (const USBH_DEV *ptr_dev, const USB_DEVICE_DESCRIPTOR *ptr_dev_desc, const USB_CONFIGURATION_DESCRIPTOR *ptr_cfg_desc);

/// \brief Callback function called when custom class device is disconnected and needs
///        to de-configure resources used by custom class device instance
/// \param[in]     instance             index of custom class device instance.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_CustomClass_Unconfigure (uint8_t instance);

/// \brief Callback function called when custom class device is connected and needs
///        to initialize custom class device instance
/// \param[in]     instance             index of custom class device instance.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_CustomClass_Initialize (uint8_t instance);

/// \brief Callback function called when custom class device is disconnected and needs
///        to de-initialize custom class device instance
/// \param[in]     instance             index of custom class device instance.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_CustomClass_Uninitialize (uint8_t instance);

/// \brief Create Pipe
/// \param[in]     ctrl                 index of USB Host controller.
/// \param[in]     dev_addr             device address.
/// \param[in]     dev_speed            device speed.
/// \param[in]     hub_addr             hub address.
/// \param[in]     hub_port             hub port.
/// \param[in]     ep_addr              endpoint address
///                 - ep_addr.0..3:       address
///                 - ep_addr.7:          direction
/// \param[in]     ep_type              endpoint type.
/// \param[in]     ep_max_packet_size   endpoint maximum packet size.
/// \param[in]     ep_interval          endpoint polling interval.
/// \return                             pointer to created pipe or pipe creation failed
///                - value > 0:         pointer to created pipe
///                - value 0:           pipe creation failed
extern USBH_PIPE *USBH_PipeCreate (uint8_t ctrl, uint8_t dev_addr, uint8_t dev_speed, uint8_t hub_addr, uint8_t hub_port, uint8_t ep_addr, uint8_t ep_type, uint16_t ep_max_packet_size, uint8_t ep_interval);

/// \brief Modify Pipe
/// \param[in]     ctrl                 index of USB Host controller.
/// \param[in]     ptr_pipe             pointer to pipe.
/// \param[in]     dev_addr             device address.
/// \param[in]     dev_speed            device speed.
/// \param[in]     hub_addr             hub address.
/// \param[in]     hub_port             hub port.
/// \param[in]     ep_max_packet_size   endpoint maximum packet size.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_PipeModify (uint8_t ctrl, USBH_PIPE *ptr_pipe, uint8_t dev_addr, uint8_t dev_speed, uint8_t hub_addr, uint8_t hub_port, uint16_t ep_max_packet_size);

/// \brief Delete Pipe
/// \param[in]     ctrl                 index of USB Host controller.
/// \param[in]     ptr_pipe             pointer to pipe.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_PipeDelete (uint8_t ctrl, USBH_PIPE *ptr_pipe);

/// \brief Reset Pipe (reset data toggle)
/// \param[in]     ctrl                 index of USB Host controller.
/// \param[in]     ptr_pipe             pointer to pipe.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_PipeReset (uint8_t ctrl, USBH_PIPE *ptr_pipe);

/// \brief Receive data on Pipe
/// \param[in]     ctrl                 index of USB Host controller.
/// \param[in]     ptr_pipe             pointer to pipe.
/// \param[out]    buf                  buffer that receives data.
/// \param[in]     len                  maximum number of bytes to receive.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_PipeReceive (uint8_t ctrl, USBH_PIPE *ptr_pipe, uint8_t *buf, uint32_t len);

/// \brief Get result of receive data operation on Pipe
/// \param[in]     ctrl                 index of USB Host controller.
/// \param[in]     ptr_pipe             pointer to pipe.
/// \return                             number of successfully received data bytes.
extern uint32_t USBH_PipeReceiveGetResult (uint8_t ctrl, const USBH_PIPE *ptr_pipe);

/// \brief Send data on Pipe
/// \param[in]     ctrl                 index of USB Host controller.
/// \param[in]     ptr_pipe             pointer to pipe.
/// \param[in]     buf                  buffer containing data bytes to send.
/// \param[in]     len                  maximum number of bytes to send.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_PipeSend (uint8_t ctrl, USBH_PIPE *ptr_pipe, const uint8_t *buf, uint32_t len);

/// \brief Get result of send data operation on Pipe
/// \param[in]     ctrl                 index of USB Host controller.
/// \param[in]     ptr_pipe             pointer to pipe.
/// \return                             number of successfully sent data bytes.
extern uint32_t USBH_PipeSendGetResult (uint8_t ctrl, const USBH_PIPE *ptr_pipe);

/// \brief Abort send/receive operation on Pipe
/// \param[in]     ctrl                 index of USB Host controller.
/// \param[in]     ptr_pipe             pointer to pipe.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_PipeAbort (uint8_t ctrl, const USBH_PIPE *ptr_pipe);

/// \brief Receive data on Default Pipe
/// \param[in]     ctrl                 index of USB Host controller.
/// \param[out]    buf                  buffer that receives data.
/// \param[in]     len                  maximum number of bytes to receive.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_DefaultPipeReceive (uint8_t ctrl, uint8_t *buf, uint32_t len);

/// \brief Get result of receive data operation on Default Pipe
/// \param[in]     ctrl                 index of USB Host controller.
/// \return                             number of successfully received data bytes.
extern uint32_t USBH_DefaultPipeReceiveGetResult (uint8_t ctrl);

/// \brief Send Setup Packet on Default Pipe
/// \param[in]     ctrl                 index of USB Host controller.
/// \param[in]     setup_packet         pointer to setup packet.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_DefaultPipeSendSetup (uint8_t ctrl, const USB_SETUP_PACKET *setup_packet);

/// \brief Send data on Default Pipe
/// \param[in]     ctrl                 index of USB Host controller.
/// \param[in]     buf                  buffer containing data bytes to send.
/// \param[in]     len                  maximum number of bytes to send.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_DefaultPipeSend (uint8_t ctrl, const uint8_t *buf, uint32_t len);

/// \brief Get result of send data operation on Default Pipe
/// \param[in]     ctrl                 index of USB Host controller.
/// \return                             number of successfully sent data bytes.
extern uint32_t USBH_DefaultPipeSendGetResult (uint8_t ctrl);

/// \brief Abort send/receive operation on Default Pipe
/// \param[in]     ctrl                 index of USB Host controller.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_DefaultPipeAbort (uint8_t ctrl);

/// \brief Do a Control Transfer on Default Pipe
/// \param[in]     ctrl                 index of USB Host controller.
/// \param[in]     setup_packet         pointer to setup packet.
/// \param[in,out] data                 buffer containing data bytes to send or where data should be received in data stage of Control Transfer.
/// \param[in]     len                  number of bytes to send or receive in data stage of Control Transfer.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_ControlTransfer (uint8_t ctrl, const USB_SETUP_PACKET *setup_packet, uint8_t *data, uint32_t len);

/// \brief Standard Device Request on Default Pipe - GET_STATUS
/// \param[in]     ctrl                 index of USB Host controller.
/// \param[in]     recipient            recipient.
/// \param[in]     index                interface or endpoint index.
/// \param[out]    ptr_stat_dat         pointer to where status data should be received.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_DeviceRequest_GetStatus (uint8_t ctrl, uint8_t recipient, uint8_t index, uint8_t *ptr_stat_dat);

/// \brief Standard Device Request on Default Pipe - CLEAR_FEATURE
/// \param[in]     ctrl                 index of USB Host controller.
/// \param[in]     recipient            recipient.
/// \param[in]     index                interface or endpoint index.
/// \param[in]     feature_selector     feature selector.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_DeviceRequest_ClearFeature (uint8_t ctrl, uint8_t recipient, uint8_t index, uint8_t feature_selector);

/// \brief Standard Device Request on Default Pipe - SET_FEATURE
/// \param[in]     ctrl                 index of USB Host controller.
/// \param[in]     recipient            recipient.
/// \param[in]     index                interface or endpoint index.
/// \param[in]     feature_selector     feature selector.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_DeviceRequest_SetFeature (uint8_t ctrl, uint8_t recipient, uint8_t index, uint8_t feature_selector);

/// \brief Standard Device Request on Default Pipe - SET_ADDRESS
/// \param[in]     ctrl                 index of USB Host controller.
/// \param[in]     device_address       device address.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_DeviceRequest_SetAddress (uint8_t ctrl, uint8_t device_address);

/// \brief Standard Device Request on Default Pipe - GET_DESCRIPTOR
/// \param[in]     ctrl                 index of USB Host controller.
/// \param[in]     recipient            recipient.
/// \param[in]     descriptor_type      descriptor type.
/// \param[in]     descriptor_index     descriptor index.
/// \param[in]     language_id          language ID.
/// \param[out]    descriptor_data      pointer to where descriptor data will be read.
/// \param[in]     descriptor_length    descriptor length.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_DeviceRequest_GetDescriptor (uint8_t ctrl, uint8_t recipient, uint8_t descriptor_type, uint8_t descriptor_index, uint8_t language_id, uint8_t *descriptor_data, uint16_t descriptor_length);

/// \brief Standard Device Request on Default Pipe - SET_DESCRIPTOR
/// \param[in]     ctrl                 index of USB Host controller.
/// \param[in]     recipient            recipient.
/// \param[in]     descriptor_type      descriptor type.
/// \param[in]     descriptor_index     descriptor index.
/// \param[in]     language_id          language ID.
/// \param[in]     descriptor_data      pointer to descriptor data to be written.
/// \param[in]     descriptor_length    descriptor length.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_DeviceRequest_SetDescriptor (uint8_t ctrl, uint8_t recipient, uint8_t descriptor_type, uint8_t descriptor_index, uint8_t language_id, uint8_t *descriptor_data, uint16_t descriptor_length);

/// \brief Standard Device Request on Default Pipe - GET_CONFIGURATION
/// \param[in]     ctrl                 index of USB Host controller.
/// \param[out]    ptr_configuration    pointer to where configuration will be read.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_DeviceRequest_GetConfiguration (uint8_t ctrl, uint8_t *ptr_configuration);

/// \brief Standard Device Request on Default Pipe - SET_CONFIGURATION
/// \param[in]     ctrl                 index of USB Host controller.
/// \param[in]     configuration        configuration.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_DeviceRequest_SetConfiguration (uint8_t ctrl, uint8_t configuration);

/// \brief Standard Device Request on Default Pipe - GET_INTERFACE
/// \param[in]     ctrl                 index of USB Host controller.
/// \param[in]     index                interface index.
/// \param[out]    ptr_alternate        pointer to where alternate setting data will be read.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_DeviceRequest_GetInterface (uint8_t ctrl, uint8_t index, uint8_t *ptr_alternate);

/// \brief Standard Device Request on Default Pipe - SET_INTERFACE
/// \param[in]     ctrl                 index of USB Host controller.
/// \param[in]     index                interface index.
/// \param[in]     alternate            alternate setting.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_DeviceRequest_SetInterface (uint8_t ctrl, uint8_t index, uint8_t alternate);

/// \brief Standard Device Request on Default Pipe - SYNCH_FRAME
/// \param[in]     ctrl                 index of USB Host controller.
/// \param[in]     index                interface or endpoint index.
/// \param[out]    ptr_frame_number     pointer to where frame number data will be read.
/// \return                             status code that indicates the execution status of the function as defined with \ref usbStatus.
extern usbStatus USBH_DeviceRequest_SynchFrame (uint8_t ctrl, uint8_t index, uint8_t *ptr_frame_number);

#ifdef __cplusplus
}
#endif

#endif
