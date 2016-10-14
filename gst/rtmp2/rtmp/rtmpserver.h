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

#ifndef _GST_RTMP_SERVER_H_
#define _GST_RTMP_SERVER_H_

#include <gio/gio.h>

#include <rtmp/rtmpconnection.h>

G_BEGIN_DECLS

#define GST_TYPE_RTMP_SERVER   (gst_rtmp_server_get_type())
#define GST_RTMP_SERVER(obj)   (G_TYPE_CHECK_INSTANCE_CAST((obj),GST_TYPE_RTMP_SERVER,GstRtmpServer))
#define GST_RTMP_SERVER_CLASS(klass)   (G_TYPE_CHECK_CLASS_CAST((klass),GST_TYPE_RTMP_SERVER,GstRtmpServerClass))
#define GST_IS_RTMP_SERVER(obj)   (G_TYPE_CHECK_INSTANCE_TYPE((obj),GST_TYPE_RTMP_SERVER))
#define GST_IS_RTMP_SERVER_CLASS(obj)   (G_TYPE_CHECK_CLASS_TYPE((klass),GST_TYPE_RTMP_SERVER))

typedef struct _GstRtmpServer GstRtmpServer;
typedef struct _GstRtmpServerClass GstRtmpServerClass;

struct _GstRtmpServer
{
  GObject object;

  /* properties */
  int port;

  /* private */
  GSocketService *socket_service;
  GList *connections;

};

struct _GstRtmpServerClass
{
  GObjectClass object_class;

  /* signals */
  void (*add_connection) (GstRtmpServer *server,
      GstRtmpConnection *connection);
  void (*remove_connection) (GstRtmpServer *server,
      GstRtmpConnection *connection);
};

GType gst_rtmp_server_get_type (void);

GstRtmpServer *gst_rtmp_server_new (void);
void gst_rtmp_server_start (GstRtmpServer * rtmpserver);
void gst_rtmp_server_add_connection (GstRtmpServer *rtmpserver,
    GstRtmpConnection *connection);
void gst_rtmp_server_remove_connection (GstRtmpServer *rtmpserver,
    GstRtmpConnection *connection);

G_END_DECLS

#endif
