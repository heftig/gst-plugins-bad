/* GStreamer RTMP Library
 * Copyright (C) 2017 Make.TV, Inc. <info@make.tv>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 51 Franklin St, Fifth Floor,
 * Boston, MA 02110-1301, USA.
 */

#ifndef _GST_RTMP_MESSAGE_H_
#define _GST_RTMP_MESSAGE_H_

#include <gst/gst.h>

G_BEGIN_DECLS

typedef enum {
  GST_RTMP_MESSAGE_TYPE_INVALID = 0,
  GST_RTMP_MESSAGE_TYPE_SET_CHUNK_SIZE = 1,
  GST_RTMP_MESSAGE_TYPE_ABORT_MESSAGE = 2,
  GST_RTMP_MESSAGE_TYPE_ACKNOWLEDGEMENT = 3,
  GST_RTMP_MESSAGE_TYPE_USER_CONTROL = 4,
  GST_RTMP_MESSAGE_TYPE_WINDOW_ACK_SIZE = 5,
  GST_RTMP_MESSAGE_TYPE_SET_PEER_BANDWIDTH = 6,
  GST_RTMP_MESSAGE_TYPE_AUDIO = 8,
  GST_RTMP_MESSAGE_TYPE_VIDEO = 9,
  GST_RTMP_MESSAGE_TYPE_DATA_AMF3 = 15,
  GST_RTMP_MESSAGE_TYPE_SHARED_OBJECT_AMF3 = 16,
  GST_RTMP_MESSAGE_TYPE_COMMAND_AMF3 = 17,
  GST_RTMP_MESSAGE_TYPE_DATA_AMF0 = 18,
  GST_RTMP_MESSAGE_TYPE_SHARED_OBJECT_AMF0 = 19,
  GST_RTMP_MESSAGE_TYPE_COMMAND_AMF0 = 20,
  GST_RTMP_MESSAGE_TYPE_AGGREGATE = 22,
} GstRtmpMessageType;

gboolean gst_rtmp_message_type_is_valid (GstRtmpMessageType type);
const gchar * gst_rtmp_message_type_get_nick (GstRtmpMessageType type);

typedef enum
{
  GST_RTMP_USER_CONTROL_TYPE_STREAM_BEGIN = 0,
  GST_RTMP_USER_CONTROL_TYPE_STREAM_EOF = 1,
  GST_RTMP_USER_CONTROL_TYPE_STREAM_DRY = 2,
  GST_RTMP_USER_CONTROL_TYPE_SET_BUFFER_LENGTH = 3,
  GST_RTMP_USER_CONTROL_TYPE_STREAM_IS_RECORDED = 4,
  GST_RTMP_USER_CONTROL_TYPE_PING_REQUEST = 6,
  GST_RTMP_USER_CONTROL_TYPE_PING_RESPONSE = 7,
} GstRtmpUserControlType;

const gchar * gst_rtmp_user_control_type_get_nick (
    GstRtmpUserControlType type);

#define GST_RTMP_META_API_TYPE (gst_rtmp_meta_api_get_type())
#define GST_RTMP_META_INFO (gst_rtmp_meta_get_info())
typedef struct _GstRtmpMeta GstRtmpMeta;

struct _GstRtmpMeta {
  GstMeta meta;
  guint32 cstream;
  guint32 ts_delta;
  guint32 size;
  GstRtmpMessageType type;
  guint32 mstream;
};

GType gst_rtmp_meta_api_get_type (void);
const GstMetaInfo * gst_rtmp_meta_get_info (void);

GstRtmpMeta * gst_buffer_add_rtmp_meta (GstBuffer * buffer);

static inline GstRtmpMeta *
gst_buffer_get_rtmp_meta (GstBuffer * buffer)
{
  return (GstRtmpMeta *) gst_buffer_get_meta (buffer, GST_RTMP_META_API_TYPE);
}


GstBuffer * gst_rtmp_message_new (GstRtmpMessageType type, guint32 cstream,
    guint32 mstream);
GstBuffer * gst_rtmp_message_new_wrapped (GstRtmpMessageType type, guint32 cstream,
    guint32 mstream, guint8 * data, gsize size);

void gst_rtmp_buffer_dump (GstBuffer * buffer, const gchar * prefix);

GstRtmpMessageType gst_rtmp_message_get_type (GstBuffer * buffer);
gboolean gst_rtmp_message_is_protocol_control (GstBuffer * buffer);
gboolean gst_rtmp_message_is_user_control (GstBuffer * buffer);

G_END_DECLS

#endif
