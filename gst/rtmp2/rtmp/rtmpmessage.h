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
  GST_RTMP_MESSAGE_TYPE_DATA = 18,
  GST_RTMP_MESSAGE_TYPE_SHARED_OBJECT = 19,
  GST_RTMP_MESSAGE_TYPE_COMMAND = 20,
  GST_RTMP_MESSAGE_TYPE_AGGREGATE = 22,
} GstRtmpMessageType;

G_END_DECLS

#endif
