/* GStreamer
 * Copyright (C) 2014 David Schleef <ds@schleef.org>
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

#ifndef _GST_RTMP2_SINK_H_
#define _GST_RTMP2_SINK_H_

#include <gst/base/gstbasesink.h>
#include <rtmp/rtmpclient.h>
#include <rtmp/rtmputils.h>

G_BEGIN_DECLS

#define GST_TYPE_RTMP2_SINK   (gst_rtmp2_sink_get_type())
#define GST_RTMP2_SINK(obj)   (G_TYPE_CHECK_INSTANCE_CAST((obj),GST_TYPE_RTMP2_SINK,GstRtmp2Sink))
#define GST_RTMP2_SINK_CLASS(klass)   (G_TYPE_CHECK_CLASS_CAST((klass),GST_TYPE_RTMP2_SINK,GstRtmp2SinkClass))
#define GST_IS_RTMP2_SINK(obj)   (G_TYPE_CHECK_INSTANCE_TYPE((obj),GST_TYPE_RTMP2_SINK))
#define GST_IS_RTMP2_SINK_CLASS(obj)   (G_TYPE_CHECK_CLASS_TYPE((klass),GST_TYPE_RTMP2_SINK))

typedef struct _GstRtmp2Sink GstRtmp2Sink;
typedef struct _GstRtmp2SinkClass GstRtmp2SinkClass;

struct _GstRtmp2Sink
{
  GstBaseSink base_rtmp2sink;

  /* properties */
  char *uri;
  int timeout;
  char *server_address;
  int port;
  char *application;
  char *stream;
  char *secure_token;

  /* stuff */
  GMutex lock;
  GCond cond;
  gboolean reset;
  GstTask *task;
  GRecMutex task_lock;
  GMainLoop *task_main_loop;

  GstRtmpClient *client;
  GstRtmpConnection *connection;
  gboolean is_connected;
  gboolean dump;

};

struct _GstRtmp2SinkClass
{
  GstBaseSinkClass base_rtmp2sink_class;
};

GType gst_rtmp2_sink_get_type (void);

G_END_DECLS

#endif
