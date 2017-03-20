/* GStreamer RTMP Library
 * Copyright (C) 2013 David Schleef <ds@schleef.org>
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

#ifndef _GST_RTMP_CONNECTION_H_
#define _GST_RTMP_CONNECTION_H_

#include <gio/gio.h>
#include <rtmp/rtmpchunk.h>
#include <rtmp/amf.h>

G_BEGIN_DECLS

#define GST_TYPE_RTMP_CONNECTION   (gst_rtmp_connection_get_type())
#define GST_RTMP_CONNECTION(obj)   (G_TYPE_CHECK_INSTANCE_CAST((obj),GST_TYPE_RTMP_CONNECTION,GstRtmpConnection))
#define GST_RTMP_CONNECTION_CLASS(klass)   (G_TYPE_CHECK_CLASS_CAST((klass),GST_TYPE_RTMP_CONNECTION,GstRtmpConnectionClass))
#define GST_IS_RTMP_CONNECTION(obj)   (G_TYPE_CHECK_INSTANCE_TYPE((obj),GST_TYPE_RTMP_CONNECTION))
#define GST_IS_RTMP_CONNECTION_CLASS(obj)   (G_TYPE_CHECK_CLASS_TYPE((klass),GST_TYPE_RTMP_CONNECTION))

typedef struct _GstRtmpConnection GstRtmpConnection;
typedef struct _GstRtmpConnectionClass GstRtmpConnectionClass;

typedef void (*GstRtmpConnectionCallback) (GstRtmpConnection *connection);
typedef void (*GstRtmpCommandCallback) (const gchar *command_name,
    GPtrArray *arguments, gpointer user_data);
typedef void (*GstRtmpConnectionGotChunkFunc)
    (GstRtmpConnection *connection, GstRtmpChunk *chunk, gpointer user_data);

GType gst_rtmp_connection_get_type (void);

GstRtmpConnection *gst_rtmp_connection_new (GSocketConnection * connection);
void gst_rtmp_connection_close (GstRtmpConnection *connection);
void gst_rtmp_connection_close_and_unref (gpointer ptr);

void gst_rtmp_connection_set_chunk_callback (GstRtmpConnection *connection,
    GstRtmpConnectionGotChunkFunc callback, gpointer user_data,
    GDestroyNotify user_data_destroy);

void gst_rtmp_connection_start_handshake (GstRtmpConnection *connection,
    gboolean is_server);
void gst_rtmp_connection_queue_chunk (GstRtmpConnection *connection,
    GstRtmpChunk *chunk);

guint gst_rtmp_connection_send_command (GstRtmpConnection *connection,
    GstRtmpCommandCallback response_command, gpointer user_data,
    guint32 stream_id, const gchar *command_name, const GstAmfNode * argument,
    ...) G_GNUC_NULL_TERMINATED;


G_END_DECLS

#endif
