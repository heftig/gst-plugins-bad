/* GStreamer RTMP Library
 * Copyright (C) 2014 David Schleef <ds@schleef.org>
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

#ifndef _GST_RTMP_AMF_H_
#define _GST_RTMP_AMF_H_

#include <glib.h>

G_BEGIN_DECLS

typedef enum
{
  GST_AMF_TYPE_INVALID = -1,
  GST_AMF_TYPE_NUMBER = 0,
  GST_AMF_TYPE_BOOLEAN = 1,
  GST_AMF_TYPE_STRING = 2,
  GST_AMF_TYPE_OBJECT = 3,
  GST_AMF_TYPE_MOVIECLIP = 4,
  GST_AMF_TYPE_NULL = 5,
  GST_AMF_TYPE_UNDEFINED = 6,
  GST_AMF_TYPE_REFERENCE = 7,
  GST_AMF_TYPE_ECMA_ARRAY = 8,
  GST_AMF_TYPE_OBJECT_END = 9,
  GST_AMF_TYPE_STRICT_ARRAY = 10,
  GST_AMF_TYPE_DATE = 11,
  GST_AMF_TYPE_LONG_STRING = 12,
  GST_AMF_TYPE_UNSUPPORTED = 13,
  GST_AMF_TYPE_RECORDSET = 14,
  GST_AMF_TYPE_XML_DOCUMENT = 15,
  GST_AMF_TYPE_TYPED_OBJECT = 16,
  GST_AMF_TYPE_AVMPLUS_OBJECT = 17
} GstAmfType;

static inline gboolean
gst_amf_type_is_valid (GstAmfType type)
{
  return type >= 0 && type <= GST_AMF_TYPE_AVMPLUS_OBJECT;
}

const gchar * gst_amf_type_get_nick (GstAmfType type);

typedef struct _GstAmfNode GstAmfNode;

GstAmfNode * gst_amf_node_new_null (void);
GstAmfNode * gst_amf_node_new_number (gdouble value);
GstAmfNode * gst_amf_node_new_boolean (gboolean value);
GstAmfNode * gst_amf_node_new_string (const gchar * value);
GstAmfNode * gst_amf_node_new_take_string (gchar * value);
GstAmfNode * gst_amf_node_new_object (void);

GstAmfNode * gst_amf_node_copy (const GstAmfNode * node);
void gst_amf_node_free (gpointer ptr);

GstAmfType gst_amf_node_get_type (const GstAmfNode * node);
gdouble gst_amf_node_get_number (const GstAmfNode * node);
gboolean gst_amf_node_get_boolean (const GstAmfNode * node);
gchar * gst_amf_node_get_string (const GstAmfNode * node);
const gchar * gst_amf_node_peek_string (const GstAmfNode * node);

const GstAmfNode * gst_amf_node_get_field (const GstAmfNode * node,
    const gchar * name);
const GstAmfNode * gst_amf_node_get_field_by_index (const GstAmfNode * node,
    guint index);
guint gst_amf_node_get_num_fields (const GstAmfNode * node);

void gst_amf_node_set_number (GstAmfNode * node, gdouble value);
void gst_amf_node_set_boolean (GstAmfNode * node, gboolean value);
void gst_amf_node_set_string (GstAmfNode * node, const gchar * value);
void gst_amf_node_set_string_take (GstAmfNode * node, gchar * value);

void gst_amf_node_append_field (GstAmfNode * node,
    const gchar * name, const GstAmfNode * value);
void gst_amf_node_append_take_field (GstAmfNode * node,
    const gchar * name, GstAmfNode * value);
void gst_amf_node_append_field_number (GstAmfNode * node,
    const gchar * name, gdouble value);
void gst_amf_node_append_field_boolean (GstAmfNode * node,
    const gchar * name, gboolean value);
void gst_amf_node_append_field_string (GstAmfNode * node,
    const gchar * name, const gchar * value);
void gst_amf_node_append_field_take_string (GstAmfNode * node,
    const gchar * name, gchar * value);

void gst_amf_node_dump (const GstAmfNode * node, gboolean multiline,
    GString * string);

GPtrArray * gst_amf_parse_command (GBytes * bytes, gdouble * transaction_id,
    gchar ** command_name);

GBytes * gst_amf_serialize_command (gdouble transaction_id,
    const gchar * command_name, const GstAmfNode * argument, ...) G_GNUC_NULL_TERMINATED;
GBytes * gst_amf_serialize_command_valist (gdouble transaction_id,
    const gchar * command_name, const GstAmfNode * argument, va_list va_args);

G_END_DECLS
#endif
