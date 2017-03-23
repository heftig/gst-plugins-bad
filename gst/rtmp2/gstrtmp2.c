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
 * Free Software Foundation, Inc., 51 Franklin Street, Suite 500,
 * Boston, MA 02110-1335, USA.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "gstrtmp2src.h"
#include "gstrtmp2sink.h"

GST_DEBUG_CATEGORY (gst_rtmp_debug_category);
GST_DEBUG_CATEGORY (gst_rtmp_amf_debug_category);
GST_DEBUG_CATEGORY (gst_rtmp_handshake_debug_category);

static gboolean
plugin_init (GstPlugin * plugin)
{
  GST_DEBUG_CATEGORY_INIT (gst_rtmp_debug_category, "rtmp2", 0,
      "debug category for rtmp2 plugin");
  GST_DEBUG_CATEGORY_INIT (gst_rtmp_amf_debug_category, "rtmpamf", 0,
      "debug category for the amf parser");
  GST_DEBUG_CATEGORY_INIT (gst_rtmp_handshake_debug_category, "rtmphandshake",
      0, "debug category for the rtmp connection handshake");
  gst_element_register (plugin, "rtmp2src", GST_RANK_PRIMARY + 1,
      GST_TYPE_RTMP2_SRC);
  gst_element_register (plugin, "rtmp2sink", GST_RANK_PRIMARY + 1,
      GST_TYPE_RTMP2_SINK);

  return TRUE;
}

GST_PLUGIN_DEFINE (GST_VERSION_MAJOR,
    GST_VERSION_MINOR,
    rtmp2,
    "RTMP plugin",
    plugin_init, VERSION, "LGPL", PACKAGE_NAME, GST_PACKAGE_ORIGIN)
