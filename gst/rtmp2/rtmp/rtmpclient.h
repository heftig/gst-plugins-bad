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

#ifndef _GST_RTMP_CLIENT_H_
#define _GST_RTMP_CLIENT_H_

#include <rtmp/rtmpmessage.h>
#include <rtmp/rtmpchunk.h>
#include <rtmp/rtmpconnection.h>

G_BEGIN_DECLS

#define GST_TYPE_RTMP_CLIENT   (gst_rtmp_client_get_type())
#define GST_RTMP_CLIENT(obj)   (G_TYPE_CHECK_INSTANCE_CAST((obj),GST_TYPE_RTMP_CLIENT,GstRtmpClient))
#define GST_RTMP_CLIENT_CLASS(klass)   (G_TYPE_CHECK_CLASS_CAST((klass),GST_TYPE_RTMP_CLIENT,GstRtmpClientClass))
#define GST_IS_RTMP_CLIENT(obj)   (G_TYPE_CHECK_INSTANCE_TYPE((obj),GST_TYPE_RTMP_CLIENT))
#define GST_IS_RTMP_CLIENT_CLASS(obj)   (G_TYPE_CHECK_CLASS_TYPE((klass),GST_TYPE_RTMP_CLIENT))

typedef struct _GstRtmpClient GstRtmpClient;
typedef struct _GstRtmpClientClass GstRtmpClientClass;

typedef void (*GstRtmpClientMessageCallback) (GstRtmpClient *client,
    GstRtmpMessage *message, gpointer user_data);
typedef void (*GstRtmpClientChunkCallback) (GstRtmpClient *client,
    GstRtmpChunk *chunk, gpointer user_data);

#define GST_RTMP_ERROR g_quark_from_static_string ("GstRtmpError")

enum {
  GST_RTMP_ERROR_TOO_LAZY = 0
};

typedef enum {
  GST_RTMP_CLIENT_STATE_NEW,
  GST_RTMP_CLIENT_STATE_CONNECTING,
  GST_RTMP_CLIENT_STATE_CONNECTED,
} GstRtmpClientState;


struct _GstRtmpClient
{
  GObject object;

  /* properties */
  char *server_address;
  int server_port;
  char *stream;
  int timeout;

  /* private */
  GstRtmpClientState state;
  GMutex lock;
  GCond cond;
  GMainContext *context;

  GSocketConnection *socket_connection;

  GstRtmpConnection *connection;
};

struct _GstRtmpClientClass
{
  GObjectClass object_class;

  /* signals */
  void (*got_chunk) (GstRtmpClient *client, GstRtmpChunk *chunk);
  void (*got_message) (GstRtmpClient *client, GstRtmpMessage *message);

};

GType gst_rtmp_client_get_type (void);

GstRtmpClient *gst_rtmp_client_new (void);
void gst_rtmp_client_set_url (GstRtmpClient *client, const char *url);
void gst_rtmp_client_set_server_address (GstRtmpClient * client, const char *host);
void gst_rtmp_client_set_server_port (GstRtmpClient * client, int port);
void gst_rtmp_client_set_stream (GstRtmpClient * client, const char *stream);

void gst_rtmp_client_connect_async (GstRtmpClient *client,
    GCancellable *cancellable, GAsyncReadyCallback callback,
    gpointer user_data);
gboolean gst_rtmp_client_connect_finish (GstRtmpClient *client,
    GAsyncResult *result, GError **error);

GstRtmpConnection *gst_rtmp_client_get_connection (GstRtmpClient *client);


G_END_DECLS

#endif
