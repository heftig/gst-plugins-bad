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
 * Free Software Foundation, Inc., 51 Franklin Street, Suite 500,
 * Boston, MA 02110-1335, USA.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <gst/gst.h>
#include "rtmpstream.h"

GST_DEBUG_CATEGORY_STATIC (gst_rtmp_stream_debug_category);
#define GST_CAT_DEFAULT gst_rtmp_stream_debug_category

/* prototypes */


static void gst_rtmp_stream_set_property (GObject * object,
    guint property_id, const GValue * value, GParamSpec * pspec);
static void gst_rtmp_stream_get_property (GObject * object,
    guint property_id, GValue * value, GParamSpec * pspec);
static void gst_rtmp_stream_dispose (GObject * object);
static void gst_rtmp_stream_finalize (GObject * object);


enum
{
  PROP_0
};

/* class initialization */

G_DEFINE_TYPE_WITH_CODE (GstRtmpStream, gst_rtmp_stream, G_TYPE_OBJECT,
    GST_DEBUG_CATEGORY_INIT (gst_rtmp_stream_debug_category, "rtmpstream", 0,
        "debug category for rtmpstream element"));

static void
gst_rtmp_stream_class_init (GstRtmpStreamClass * klass)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (klass);

  gobject_class->set_property = gst_rtmp_stream_set_property;
  gobject_class->get_property = gst_rtmp_stream_get_property;
  gobject_class->dispose = gst_rtmp_stream_dispose;
  gobject_class->finalize = gst_rtmp_stream_finalize;

}

static void
gst_rtmp_stream_init (GstRtmpStream * rtmpstream)
{
}

void
gst_rtmp_stream_set_property (GObject * object, guint property_id,
    const GValue * value, GParamSpec * pspec)
{
  GstRtmpStream *rtmpstream = GST_RTMP_STREAM (object);

  GST_DEBUG_OBJECT (rtmpstream, "set_property");

  switch (property_id) {
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, property_id, pspec);
      break;
  }
}

void
gst_rtmp_stream_get_property (GObject * object, guint property_id,
    GValue * value, GParamSpec * pspec)
{
  GstRtmpStream *rtmpstream = GST_RTMP_STREAM (object);

  GST_DEBUG_OBJECT (rtmpstream, "get_property");

  switch (property_id) {
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, property_id, pspec);
      break;
  }
}

void
gst_rtmp_stream_dispose (GObject * object)
{
  GstRtmpStream *rtmpstream = GST_RTMP_STREAM (object);

  GST_DEBUG_OBJECT (rtmpstream, "dispose");

  /* clean up as possible.  may be called multiple times */

  G_OBJECT_CLASS (gst_rtmp_stream_parent_class)->dispose (object);
}

void
gst_rtmp_stream_finalize (GObject * object)
{
  GstRtmpStream *rtmpstream = GST_RTMP_STREAM (object);

  GST_DEBUG_OBJECT (rtmpstream, "finalize");

  /* clean up object here */

  G_OBJECT_CLASS (gst_rtmp_stream_parent_class)->finalize (object);
}
