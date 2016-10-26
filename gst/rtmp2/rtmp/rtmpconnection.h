/* GStreamer RTMP Library
 * Copyright (C) 2013 David Schleef <ds@schleef.org>
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
typedef void (*GstRtmpCommandCallback) (GstRtmpConnection *connection,
    GstRtmpChunk *chunk, const char *command_name, int transaction_id,
    GstAmfNode *command_object, GstAmfNode *optional_args,
    gpointer user_data);

struct _GstRtmpConnection
{
  GObject object;

  /* should be properties */
  gboolean input_paused;
  gboolean closed;

  /* private */
  GThread *thread;
  GSocketConnection *connection;
  GCancellable *cancellable;
  int state;
  GSocketClient *socket_client;
  GAsyncQueue *output_queue;
  gboolean writing;
  GMainContext *main_context;

  GSource *input_source;
  GSource *output_source;
  GByteArray *input_bytes;
  gsize input_needed_bytes;
  GstRtmpConnectionCallback input_callback;
  gboolean handshake_complete;
  GstRtmpChunkCache *input_chunk_cache;
  GList *command_callbacks;

  /* chunk currently being written */
  GstRtmpChunk *output_chunk;
  GBytes *output_bytes;

  /* RTMP configuration */
  gsize in_chunk_size;
  gsize out_chunk_size;
  gsize window_ack_size;
  gsize total_input_bytes;
  gsize bytes_since_ack;
  gsize peer_bandwidth;
};

struct _GstRtmpConnectionClass
{
  GObjectClass object_class;
};

GType gst_rtmp_connection_get_type (void);

GstRtmpConnection *gst_rtmp_connection_new (void);
void gst_rtmp_connection_set_socket_connection (
    GstRtmpConnection *rtmpconnection, GSocketConnection *connection);
void gst_rtmp_connection_close (GstRtmpConnection *connection);
void gst_rtmp_connection_close_and_unref (gpointer ptr);

void gst_rtmp_connection_start_handshake (GstRtmpConnection *connection,
    gboolean is_server);
void gst_rtmp_connection_queue_chunk (GstRtmpConnection *connection,
    GstRtmpChunk *chunk);
void gst_rtmp_connection_dump (GstRtmpConnection *connection);

int gst_rtmp_connection_send_command (GstRtmpConnection *connection,
    int chunk_stream_id, const char *command_name, int transaction_id,
    GstAmfNode *command_object, GstAmfNode *optional_args,
    GstRtmpCommandCallback response_command, gpointer user_data);
int gst_rtmp_connection_send_command2 (GstRtmpConnection *connection,
    int chunk_stream_id, int stream_id, const char *command_name,
    int transaction_id, GstAmfNode *command_object, GstAmfNode *optional_args,
    GstAmfNode *n3, GstAmfNode *n4,
    GstRtmpCommandCallback response_command, gpointer user_data);


G_END_DECLS

#endif
