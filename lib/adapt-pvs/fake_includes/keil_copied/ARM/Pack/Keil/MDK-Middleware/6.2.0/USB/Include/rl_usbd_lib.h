/*------------------------------------------------------------------------------
 * MDK Middleware - Component ::USB:Device
 * Copyright (c) 2004-2014 ARM Germany GmbH. All rights reserved.
 *------------------------------------------------------------------------------
 * Name:    rl_usbd_lib.h
 * Purpose: USB Device header file
 * Rev.:    V6.2
 *----------------------------------------------------------------------------*/

#ifndef __RL_USBD_LIB_H__
#define __RL_USBD_LIB_H__

#include <stdint.h>
#include "usb_cdc.h"
#include "usb_msc.h"
#include "Driver_USBD.h"


/* -------------------  Start of section using anonymous unions  ------------------ */
#if defined(__CC_ARM)
  #pragma push
  #pragma anon_unions
#elif defined(__ICCARM__)
  #pragma language=extended
#elif defined(__GNUC__)
  /* anonymous unions are enabled by default */
#elif defined(__TMS470__)
  /* anonymous unions are enabled by default */
#elif defined(__TASKING__)
  #pragma warning 586
#else
  #warning Not supported compiler type
#endif


//  ==== USB Device Internal Structures ====

/// Structure used for Control Endpoint 0 communication
typedef struct _usbd_ep_data_t {
  uint8_t              *data;                               ///< data buffer for send or receive
  uint16_t              cnt;                                ///< number of bytes in data buffer (to send or received)
} usbd_ep_data_t;

/// Structure containing device information
typedef struct _usbd_data_t {
  uint16_t              device_status;                      ///< device status (remote wakeup, self powered)
  uint8_t               device_address;                     ///< device address on the bus
  uint8_t               configuration;                      ///< active configuration
  uint8_t               interface;                          ///< active interface
  uint32_t              endpoint_mask;                      ///< mask containing active endpoints
  uint32_t              endpoint_halt;                      ///< mask containing halted endpoints
  uint32_t              endpoint_stall;                     ///< mask containing stalled endpoints
  uint32_t              endpoint_active;                    ///< mask containing endpoints with active transfers
  uint8_t               num_interfaces;                     ///< number of available interfaces
  union {
    uint8_t             flags;                              ///< status flags
    struct {
      uint8_t           high_speed : 1;                     ///< information if device is in high speed
      uint8_t           zlp        : 1;                     ///< zero length packet flag
      uint8_t           ext_handle : 1;                     ///< externally handled request
    };
  };

  usbd_ep_data_t        ep0_data;                           ///< Control Endpoint 0 structure
  USB_SETUP_PACKET      setup_packet;                       ///< Setup Packet structure
  uint8_t              *buf;                                ///< Buffer for in/out data
  int32_t               len;                                ///< Length for in/out data
  int32_t               len_cur;                            ///< Currently transferred bytes for in/out data
} usbd_data_t;

/// Structure containing controller instance information for stack
typedef struct _usbd_dev_t {
  ARM_DRIVER_USBD      *driver;                             ///< pointer to driver
  uint8_t              *alt_setting;                        ///< pointer to alternate settings
  uint8_t              *ep0_buf;                            ///< pointer to Control Endpoint 0 buffer
  usbd_data_t          *data_ptr;                           ///< pointer to structure containing device information
  uint8_t               bmattributes;                       ///< bmAttributes configuration setting
  uint8_t               hs;                                 ///< high-speed capability setting
  uint16_t              if_num;                             ///< number of interfaces
  uint8_t               ep_num;                             ///< number of endpoints
  uint8_t               max_packet0;                        ///< maximum packet size for Control Endpoint 0
} const usbd_dev_t;

/// Structure containing configuration values for Custom Class device instance
typedef struct _usbd_custom_class_t {
  uint8_t               dev_num;                            ///< device instance configuration setting
  uint8_t               if0_num;                            ///< interface 0 number
  uint8_t               if1_num;                            ///< interface 1 number
  uint8_t               if2_num;                            ///< interface 2 number
  uint8_t               if3_num;                            ///< interface 3 number
  uint8_t               ep_msk;                             ///< mask of used endpoints
  uint8_t               if0_ep_bulk_out;                    ///< interface 0 bulk out endpoint number
  uint8_t               if0_ep_bulk_in;                     ///< interface 0 bulk in endpoint number
  uint8_t               if0_ep_int_out;                     ///< interface 0 interrupt out endpoint number
  uint8_t               if0_ep_int_in;                      ///< interface 0 interrupt in endpoint number
  uint8_t               if0_ep_iso_out;                     ///< interface 0 isochronous out endpoint number
  uint8_t               if0_ep_iso_in;                      ///< interface 0 isochronous in endpoint number
  uint8_t               if1_ep_bulk_out;                    ///< interface 1 bulk out endpoint number
  uint8_t               if1_ep_bulk_in;                     ///< interface 1 bulk in endpoint number
  uint8_t               if1_ep_int_out;                     ///< interface 1 interrupt out endpoint number
  uint8_t               if1_ep_int_in;                      ///< interface 1 interrupt in endpoint number
  uint8_t               if1_ep_iso_out;                     ///< interface 1 isochronous out endpoint number
  uint8_t               if1_ep_iso_in;                      ///< interface 1 isochronous in endpoint number
  uint8_t               if2_ep_bulk_out;                    ///< interface 2 bulk out endpoint number
  uint8_t               if2_ep_bulk_in;                     ///< interface 2 bulk in endpoint number
  uint8_t               if2_ep_int_out;                     ///< interface 2 interrupt out endpoint number
  uint8_t               if2_ep_int_in;                      ///< interface 2 interrupt in endpoint number
  uint8_t               if2_ep_iso_out;                     ///< interface 2 isochronous out endpoint number
  uint8_t               if2_ep_iso_in;                      ///< interface 2 isochronous in endpoint number
  uint8_t               if3_ep_bulk_out;                    ///< interface 3 bulk out endpoint number
  uint8_t               if3_ep_bulk_in;                     ///< interface 3 bulk in endpoint number
  uint8_t               if3_ep_int_out;                     ///< interface 3 interrupt out endpoint number
  uint8_t               if3_ep_int_in;                      ///< interface 3 interrupt in endpoint number
  uint8_t               if3_ep_iso_out;                     ///< interface 3 isochronous out endpoint number
  uint8_t               if3_ep_iso_in;                      ///< interface 3 isochronous in endpoint number
} const usbd_custom_class_t;

/// Structure containing runtime values for ADC device speaker instance
typedef struct _usbd_adc_spkr_data_t {
  uint8_t               active;                             ///< streaming interface active
  uint8_t               mute;                               ///< mute state
  uint16_t              vol_cur[2];                         ///< volume current value
  uint16_t              vol_min[2];                         ///< volume minimum value
  uint16_t              vol_max[2];                         ///< volume maximum value
  uint16_t              vol_res[2];                         ///< volume resolution
  uint8_t               receive_active;                     ///< reception of data on the USB Bus active
  uint8_t              *ptr_data_received;                  ///< pointer to receive intermediate buffer to received unread data
  int32_t               data_received_cnt;                  ///< number of received bytes (cumulative)
  uint8_t              *ptr_data_read;                      ///< pointer to receive intermediate buffer to received read data
  int32_t               data_read_cnt;                      ///< number of read bytes (cumulative)
} usbd_adc_spkr_data_t;

/// Structure containing runtime values for ADC device microphone instance
typedef struct _usbd_adc_mic_data_t {
  uint8_t               active;                             ///< streaming interface active
  uint8_t               mute;                               ///< mute state
  uint16_t              vol_cur[2];                         ///< volume current value
  uint16_t              vol_min[2];                         ///< volume minimum value
  uint16_t              vol_max[2];                         ///< volume maximum value
  uint16_t              vol_res[2];                         ///< volume resolution
  uint8_t               send_active;                        ///< sending of data on the USB Bus active
  uint8_t              *ptr_data_to_send;                   ///< pointer to send intermediate buffer to data to be sent
  int32_t               data_to_send_cnt;                   ///< number of bytes to send (cumulative)
  uint8_t              *ptr_data_sent;                      ///< pointer to send intermediate buffer to data already sent
  int32_t               data_sent_cnt;                      ///< number of bytes sent (cumulative)
} usbd_adc_mic_data_t;

/// Structure containing configuration values for ADC device (out or in) instance
typedef struct _usbd_adc_code_t {
  uint8_t              *data_buf;                           ///< data buffer for audio samples
  uint8_t               sif_num;                            ///< audio streaming (zero bandwidth) interface number (operational is next)
  uint8_t               fu_id;                              ///< feature unit ID
  uint8_t               ep_iso;                             ///< isochronous endpoint number
  uint32_t              ch_num;                             ///< number of channels (1 = mono, 2 = stereo)
  uint32_t              data_freq;                          ///< data frequency setting
  uint32_t              sample_sz;                          ///< sample size (in bytes)
  uint32_t              sample_res;                         ///< sample resolution (in bits)
  uint32_t              data_buf_sz;                        ///< data buffer size
  uint16_t              max_packet_size;                    ///< maximum packet size for isochronous endpoint (for LS/FS)
} const usbd_adc_code_t;

/// Structure containing configuration values for ADC device instance
typedef struct _usbd_adc_t {
  usbd_adc_spkr_data_t *spkr_data_ptr;                      ///< pointer to structure containing speaker (out) runtime values
  usbd_adc_code_t      *out_cfg;                            ///< pointer to structure containing out configuration settings
  usbd_adc_mic_data_t  *mic_data_ptr;                       ///< pointer to structure containing microphone (in) runtime values
  usbd_adc_code_t      *in_cfg;                             ///< pointer to structure containing in configuration settings
  uint8_t               dev_num;                            ///< device instance configuration setting
  uint8_t               cif_num;                            ///< audio control interface number
} const usbd_adc_t;

/// Structure containing runtime values for CDC device instance
typedef struct _usbd_cdc_data_t {
  struct {
    uint32_t            send_active    : 1;                 ///< flag active while data is being sent
    uint32_t            send_zlp       : 1;                 ///< flag active when zero length packet needs to be sent
    uint32_t            receive_active : 1;                 ///< flag active while data is being received
    uint32_t            notify_active  : 1;                 ///< flag active while notification is being sent
  };
  uint8_t              *ptr_data_to_send;                   ///< pointer to send intermediate buffer to data to be sent
  int32_t               data_to_send_cnt;                   ///< number of bytes to send (cumulative)
  uint8_t              *ptr_data_sent;                      ///< pointer to send intermediate buffer to data already sent
  int32_t               data_sent_cnt;                      ///< number of bytes sent (cumulative)
  uint8_t              *ptr_data_received;                  ///< pointer to receive intermediate buffer to received unread data
  int32_t               data_received_cnt;                  ///< number of bytes received (cumulative)
  uint8_t              *ptr_data_read;                      ///< pointer to receive intermediate buffer to received read data
  int32_t               data_read_cnt;                      ///< number of bytes read (cumulative)
  uint16_t              control_line_state;                 ///< control line state settings bitmap (bit 0: DTR state, bit 1: - RTS state)
  CDC_LINE_CODING       line_coding;                        ///< communication settings */
} usbd_cdc_data_t;

/// Structure containing configuration values for CDC device instance
typedef struct _usbd_cdc_t {
  uint8_t              *send_buf;                           ///< send data buffer
  uint8_t              *receive_buf;                        ///< receive data buffer
  uint8_t              *notify_buf;                         ///< notify data buffer
  usbd_cdc_data_t      *data_ptr;                           ///< pointer to structure containing runtime values
  uint8_t               dev_num;                            ///< device instance configuration setting
  uint8_t               cif_num;                            ///< communication class interface number
  uint8_t               dif_num;                            ///< data class interface number
  uint8_t               ep_int_in;                          ///< interrupt in endpoint number
  uint8_t               ep_bulk_in;                         ///< bulk in endpoint number
  uint8_t               ep_bulk_out;                        ///< bulk out endpoint number
  uint16_t              send_buf_sz;                        ///< maximum size of send buffer
  uint16_t              receive_buf_sz;                     ///< maximum size of receive buffer
  uint16_t              max_packet_size [2];                ///< maximum packet size for interrupt endpoint (for LS/FS and HS)
  uint16_t              max_packet_size1[2];                ///< maximum packet size for bulk endpoints (for LS/FS and HS)
} const usbd_cdc_t;

/// Structure containing runtime values for HID device instance
typedef struct _usbd_hid_data_t {
  uint8_t               protocol;                           ///< active protocol
  bool                  data_out_async_req;                 ///< request to asynchronously send data flag
  bool                  data_out_in_progress;               ///< data out in progress flag
  uint32_t              data_out_update_req_mask;           ///< request to update send data flag
  uint8_t              *ptr_data_out;                       ///< send data buffer
  volatile uint16_t     data_out_to_send_len;               ///< length of data to be sent
  uint16_t              data_out_sent_len;                  ///< length of data already sent
  bool                  data_out_end_with_short_packet;     ///< data send ended with short packet flag
  uint8_t              *ptr_data_in;                        ///< receive data buffer
  uint16_t              data_in_rece_len;                   ///< length of received data
  uint8_t              *ptr_data_feat;                      ///< feature data buffer
  uint16_t              data_feat_rece_len;                 ///< length of received feature data
  uint16_t              polling_count;                      ///< polling count used for data update
} usbd_hid_data_t;

/// Structure containing configuration values for HID device instance
typedef struct _usbd_hid_t {
  uint16_t             *idle_count;                         ///< pointer to idle count data for each report
  uint16_t             *idle_reload;                        ///< pointer to idle reload data for each report
  uint8_t              *idle_set;                           ///< pointer to idle set data (using SetIdle) for each report
  uint8_t              *in_report;                          ///< input report data buffer for each report
  uint8_t              *out_report;                         ///< output report data buffer for each report
  uint8_t              *feat_report;                        ///< feature report data buffer for each report
  usbd_hid_data_t      *data_ptr;                           ///< pointer to structure containing runtime values for each report
  uint8_t               dev_num;                            ///< device instance configuration setting
  uint8_t               if_num;                             ///< interface number
  uint8_t               ep_int_in;                          ///< interrupt in endpoint number
  uint8_t               ep_int_out;                         ///< interrupt out endpoint number (0 if not used)
  uint16_t              interval       [2];                 ///< polling interval configuration setting (for LS/FS and HS)
  uint16_t              max_packet_size[2];                 ///< maximum packet size interrupt endpoints (for LS/FS and HS)
  uint8_t               in_report_num;                      ///< number of input reports
  uint8_t               out_report_num;                     ///< number of output reports
  uint16_t              in_report_max_sz;                   ///< maximum input report size
  uint16_t              out_report_max_sz;                  ///< maximum output report size
  uint16_t              feat_report_max_sz;                 ///< maximum feature report size
} const usbd_hid_t;

/// Structure containing values for HID descriptor
typedef struct _usbd_hid_desc_t {
  uint8_t              *report_descriptor;                  ///< report descriptor
  uint16_t              report_descriptor_size;             ///< report descriptor size
  uint16_t              hid_descriptor_offset;              ///< HID descriptor offset in device descriptor
} usbd_hid_desc_t;

/// Structure containing all descriptors (except report descriptor)
typedef struct _usbd_desc_t {
  uint8_t              *ep0_descriptor;                     ///< Control Endpoint 0 descriptor
  uint8_t              *device_descriptor;                  ///< device descriptor
  uint8_t              *device_qualifier_fs;                ///< device qualifier for low/full-speed
  uint8_t              *device_qualifier_hs;                ///< device qualifier for high-speed
  uint8_t              *config_descriptor_fs;               ///< configuration descriptor for low/full-speed
  uint8_t              *config_descriptor_hs;               ///< configuration descriptor for high-speed
  uint8_t              *other_speed_config_descriptor_fs;   ///< other speed configuration descriptor for low/full-speed
  uint8_t              *other_speed_config_descriptor_hs;   ///< other speed configuration descriptor for high-speed
  uint8_t              *string_descriptor;                  ///< string descriptors
} usbd_desc_t;

/* --------------------  End of section using anonymous unions  ------------------- */
#if defined(__CC_ARM)
  #pragma pop
#elif defined(__ICCARM__)
  /* leave anonymous unions enabled */
#elif defined(__GNUC__)
  /* anonymous unions are enabled by default */
#elif defined(__TMS470__)
  /* anonymous unions are enabled by default */
#elif defined(__TASKING__)
  #pragma warning restore
#else
  #warning Not supported compiler type
#endif

#endif
