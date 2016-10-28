/* GStreamer RTMP Library
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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <string.h>
#include <gst/gst.h>

#include "amf.h"
#include "rtmpdebug.h"

#define MAX_RECURSION_DEPTH 16

typedef struct _AmfObjectField AmfObjectField;
struct _AmfObjectField
{
  gchar *name;
  GstAmfNode *value;
};

typedef struct _AmfParser AmfParser;
struct _AmfParser
{
  const guint8 *data;
  gsize size;
  gsize offset;
  gboolean error;
  guint8 recursion_depth;
};

typedef struct _AmfSerializer AmfSerializer;
struct _AmfSerializer
{
  guint8 *data;
  gsize size;
  gsize offset;
  gboolean error;
};

static char *_parse_utf8_string (AmfParser * parser);
static void _parse_object (AmfParser * parser, GstAmfNode * node);
static GstAmfNode *_parse_value (AmfParser * parser);
static void amf_object_field_free (AmfObjectField * field);
static void _serialize_object (AmfSerializer * serializer, GstAmfNode * node);
static void _serialize_value (AmfSerializer * serializer, GstAmfNode * node);


GstAmfNode *
gst_amf_node_new (GstAmfType type)
{
  GstAmfNode *node;

  node = g_slice_alloc0 (sizeof (GstAmfNode));
  node->type = type;
  if (node->type == GST_AMF_TYPE_OBJECT ||
      node->type == GST_AMF_TYPE_ECMA_ARRAY) {
    node->array_val = g_ptr_array_new ();
  }

  return node;
}

void
gst_amf_node_free (GstAmfNode * node)
{
  if (node->type == GST_AMF_TYPE_STRING) {
    g_free (node->string_val);
  } else if (node->type == GST_AMF_TYPE_OBJECT ||
      node->type == GST_AMF_TYPE_ECMA_ARRAY) {
    g_ptr_array_foreach (node->array_val, (GFunc) amf_object_field_free, NULL);
    g_ptr_array_free (node->array_val, TRUE);
  }

  g_slice_free (GstAmfNode, node);
}

static guint8
_parse_u8 (AmfParser * parser)
{
  guint8 x;
  x = parser->data[parser->offset];
  parser->offset++;
  return x;
}

static guint16
_parse_u16 (AmfParser * parser)
{
  guint16 x;
  x = GST_READ_UINT16_BE (parser->data + parser->offset);
  parser->offset += 2;
  return x;
}

static guint32
_parse_u32 (AmfParser * parser)
{
  guint32 x;
  x = GST_READ_UINT32_BE (parser->data + parser->offset);
  parser->offset += 4;
  return x;
}

static double
_parse_number (AmfParser * parser)
{
  gdouble d;
  gint i;
  guint8 *d_ptr = (guint8 *) & d;
  if (parser->offset + 8 > parser->size) {
    GST_ERROR ("number too long");
    parser->error = TRUE;
    return 0.0;
  }
  for (i = 0; i < 8; i++) {
    d_ptr[i] = parser->data[parser->offset + (7 - i)];
  }
  parser->offset += 8;
  return d;
}

static gchar *
_parse_utf8_string (AmfParser * parser)
{
  gsize size;
  gchar *s;

  size = _parse_u16 (parser);
  if (parser->offset + size > parser->size) {
    GST_ERROR ("string too long");
    parser->error = TRUE;
    return NULL;
  }
  s = g_strndup ((gchar *) (parser->data + parser->offset), size);
  parser->offset += size;

  return s;
}

static void
_parse_object (AmfParser * parser, GstAmfNode * node)
{
  while (1) {
    gchar *s;
    GstAmfNode *child_node;
    s = _parse_utf8_string (parser);
    child_node = _parse_value (parser);
    if (child_node->type == GST_AMF_TYPE_OBJECT_END) {
      g_free (s);
      gst_amf_node_free (child_node);
      break;
    }
    gst_amf_object_append_take (node, s, child_node);
    g_free (s);
  }
}

static void
_parse_ecma_array (AmfParser * parser, GstAmfNode * node)
{
  guint32 n_elements;
  guint32 i;

  n_elements = _parse_u32 (parser);

  /* FIXME This is weird.  The one time I've seen this, the encoded value
   * was 0, but the number of elements was 1. */
  if (n_elements != 0) {
    GST_ERROR ("unimplemented, n_elements != 0");
  }

  if (n_elements == 0)
    n_elements++;

  for (i = 0; i < n_elements; i++) {
    gchar *s;
    GstAmfNode *child_node;
    s = _parse_utf8_string (parser);
    child_node = _parse_value (parser);
    gst_amf_object_append_take (node, s, child_node);
    g_free (s);
  }

  // skip 3 bytes object-end-type
  parser->offset += 3;
}

static GstAmfNode *
_parse_value (AmfParser * parser)
{
  GstAmfNode *node = NULL;
  GstAmfType type;

  type = _parse_u8 (parser);
  node = gst_amf_node_new (type);

  parser->recursion_depth++;

  if (parser->recursion_depth > MAX_RECURSION_DEPTH) {
    GST_ERROR ("maximum recursion depth %d reached, ignore node",
        parser->recursion_depth);
    return node;
  }

  GST_DEBUG ("parsing type %d", type);


  switch (type) {
    case GST_AMF_TYPE_NUMBER:
      gst_amf_node_set_number (node, _parse_number (parser));
      break;
    case GST_AMF_TYPE_BOOLEAN:
      gst_amf_node_set_boolean (node, _parse_u8 (parser));
      break;
    case GST_AMF_TYPE_STRING:
      gst_amf_node_set_string_take (node, _parse_utf8_string (parser));
      break;
    case GST_AMF_TYPE_OBJECT:
      _parse_object (parser, node);
      break;
    case GST_AMF_TYPE_MOVIECLIP:
      GST_ERROR ("unimplemented AMF type: movie clip");
      break;
    case GST_AMF_TYPE_NULL:
      break;
    case GST_AMF_TYPE_ECMA_ARRAY:
      _parse_ecma_array (parser, node);
      break;
    case GST_AMF_TYPE_OBJECT_END:
      break;
    default:
      GST_ERROR ("unimplemented AMF type %d", type);
      break;
  }

  parser->recursion_depth--;

  return node;
}

GstAmfNode *
gst_amf_node_new_parse (const guint8 * data, gsize size, gsize * n_bytes)
{
  AmfParser _p = { 0 }, *parser = &_p;
  GstAmfNode *node;

  parser->data = data;
  parser->size = size;
  parser->error = FALSE;
  parser->recursion_depth = 0;
  node = _parse_value (parser);

  if (n_bytes)
    *n_bytes = parser->offset;
  return node;
}

void
gst_amf_node_set_boolean (GstAmfNode * node, gboolean val)
{
  g_return_if_fail (node->type == GST_AMF_TYPE_BOOLEAN);
  node->int_val = val;
}

void
gst_amf_node_set_number (GstAmfNode * node, gdouble val)
{
  g_return_if_fail (node->type == GST_AMF_TYPE_NUMBER);
  node->double_val = val;
}

void
gst_amf_node_set_string (GstAmfNode * node, const gchar * s)
{
  g_return_if_fail (node->type == GST_AMF_TYPE_STRING);
  node->string_val = g_strdup (s);
}

void
gst_amf_node_set_string_take (GstAmfNode * node, gchar * s)
{
  g_return_if_fail (node->type == GST_AMF_TYPE_STRING);
  node->string_val = s;
}

void
gst_amf_node_set_binary_string_take (GstAmfNode * node, guint8 * s, gsize size)
{
  g_return_if_fail (node->type == GST_AMF_TYPE_STRING);
  /* FIXME this needs to take NUL chars in string */
  node->string_val = (gchar *) s;
}

void
gst_amf_object_append_take (GstAmfNode * node, const gchar * s,
    GstAmfNode * child_node)
{
  AmfObjectField *field;

  g_return_if_fail (node->type == GST_AMF_TYPE_OBJECT ||
      node->type == GST_AMF_TYPE_ECMA_ARRAY);

  field = g_slice_alloc0 (sizeof (AmfObjectField));
  field->name = g_strdup (s);
  field->value = child_node;
  g_ptr_array_add (node->array_val, field);
}

static void
amf_object_field_free (AmfObjectField * field)
{
  g_free (field->name);
  gst_amf_node_free (field->value);
  g_slice_free (AmfObjectField, field);
}

void
gst_amf_node_set_ecma_array (GstAmfNode * node, guint8 * data, guint32 size)
{
  node->string_val = (char *) data;
  node->int_val = size;
}

void
gst_amf_object_set_number (GstAmfNode * node, const gchar * field_name,
    gdouble val)
{
  GstAmfNode *child_node;

  child_node = gst_amf_node_new (GST_AMF_TYPE_NUMBER);
  gst_amf_node_set_number (child_node, val);
  gst_amf_object_append_take (node, field_name, child_node);
}

void
gst_amf_object_set_string_take (GstAmfNode * node, const gchar * field_name,
    gchar * s)
{
  GstAmfNode *child_node;

  child_node = gst_amf_node_new (GST_AMF_TYPE_STRING);
  gst_amf_node_set_string_take (child_node, s);
  gst_amf_object_append_take (node, field_name, child_node);
}

void
gst_amf_object_set_string (GstAmfNode * node, const gchar * field_name,
    const gchar * s)
{
  gst_amf_object_set_string_take (node, field_name, g_strdup (s));
}


static void
_gst_amf_node_dump (GstAmfNode * node, guint indent)
{
  guint i;

  switch (node->type) {
    case GST_AMF_TYPE_NUMBER:
      g_print ("%g", node->double_val);
      break;
    case GST_AMF_TYPE_BOOLEAN:
      g_print ("%s", node->int_val ? "True" : "False");
      break;
    case GST_AMF_TYPE_STRING:
      g_print ("\"%s\"", node->string_val);
      break;
    case GST_AMF_TYPE_OBJECT:
    case GST_AMF_TYPE_ECMA_ARRAY:
      g_print ("{\n");
      for (i = 0; i < gst_amf_node_get_object_length (node); i++) {
        AmfObjectField *field = g_ptr_array_index (node->array_val, i);
        g_print ("%*.*s  \"%s\": ", indent, indent, "", field->name);
        _gst_amf_node_dump (field->value, indent + 2);
        g_print (",\n");
      }
      g_print ("%*.*s}", indent, indent, "");
      break;
    case GST_AMF_TYPE_MOVIECLIP:
      g_print ("MOVIE_CLIP");
      break;
    case GST_AMF_TYPE_NULL:
      g_print ("Null");
      break;
    case GST_AMF_TYPE_OBJECT_END:
      break;
    default:
      GST_ERROR ("unimplemented AMF type: %d", node->type);
      break;
  }
}

void
gst_amf_node_dump (GstAmfNode * node)
{
  _gst_amf_node_dump (node, 0);
  g_print ("\n");
}

static gboolean
_serialize_check (AmfSerializer * serializer, gsize value)
{
  if (serializer->offset + value > serializer->size) {
    serializer->error = TRUE;
  }
  return !serializer->error;
}

static void
_serialize_u8 (AmfSerializer * serializer, guint8 value)
{
  if (_serialize_check (serializer, 1)) {
    serializer->data[serializer->offset] = value;
    serializer->offset++;
  }
}

static void
_serialize_u16 (AmfSerializer * serializer, guint16 value)
{
  if (_serialize_check (serializer, 2)) {
    GST_WRITE_UINT16_BE (serializer->data + serializer->offset, value);
    serializer->offset += 2;
  }
}

static void
_serialize_u32 (AmfSerializer * serializer, guint32 value)
{
  if (_serialize_check (serializer, 4)) {
    GST_WRITE_UINT32_BE (serializer->data + serializer->offset, value);
    serializer->offset += 4;
  }
}

static void
_serialize_number (AmfSerializer * serializer, gdouble value)
{
  if (_serialize_check (serializer, 8)) {
    guint8 *d_ptr = (guint8 *) & value;
    gsize i;

    for (i = 0; i < 8; i++) {
      serializer->data[serializer->offset + i] = d_ptr[7 - i];
    }
    serializer->offset += 8;
  }
}

static void
_serialize_utf8_string (AmfSerializer * serializer, const gchar * s)
{
  guint16 size;

  size = strlen (s);
  if (_serialize_check (serializer, 2 + size)) {
    GST_WRITE_UINT16_BE (serializer->data + serializer->offset, size);
    memcpy (serializer->data + serializer->offset + 2, s, size);
    serializer->offset += 2 + size;
  }
}

static void
_serialize_object (AmfSerializer * serializer, GstAmfNode * node)
{
  guint i;

  for (i = 0; i < gst_amf_node_get_object_length (node); i++) {
    AmfObjectField *field = g_ptr_array_index (node->array_val, i);
    _serialize_utf8_string (serializer, field->name);
    _serialize_value (serializer, field->value);
  }
  _serialize_u16 (serializer, 0);
  _serialize_u8 (serializer, GST_AMF_TYPE_OBJECT_END);
}

static void
_serialize_ecma_array (AmfSerializer * serializer, GstAmfNode * node)
{
  guint i;

  _serialize_u32 (serializer, 0);
  for (i = 0; i < gst_amf_node_get_object_length (node); i++) {
    AmfObjectField *field = g_ptr_array_index (node->array_val, i);
    _serialize_utf8_string (serializer, field->name);
    _serialize_value (serializer, field->value);
  }
  _serialize_u16 (serializer, 0);
  _serialize_u8 (serializer, GST_AMF_TYPE_OBJECT_END);
}

static void
_serialize_value (AmfSerializer * serializer, GstAmfNode * node)
{
  _serialize_u8 (serializer, node->type);
  switch (node->type) {
    case GST_AMF_TYPE_NUMBER:
      _serialize_number (serializer, node->double_val);
      break;
    case GST_AMF_TYPE_BOOLEAN:
      _serialize_u8 (serializer, ! !node->int_val);
      break;
    case GST_AMF_TYPE_STRING:
      _serialize_utf8_string (serializer, node->string_val);
      break;
    case GST_AMF_TYPE_OBJECT:
      _serialize_object (serializer, node);
      break;
    case GST_AMF_TYPE_MOVIECLIP:
      GST_ERROR ("unimplemented AMF type: movie clip");
      serializer->error = TRUE;
      break;
    case GST_AMF_TYPE_NULL:
      break;
    case GST_AMF_TYPE_ECMA_ARRAY:
      _serialize_ecma_array (serializer, node);
      break;
    case GST_AMF_TYPE_OBJECT_END:
      break;
    default:
      GST_ERROR ("unimplemented AMF type %d", node->type);
      serializer->error = TRUE;
      break;
  }
}

GBytes *
gst_amf_serialize_command (const gchar * command_name, gdouble transaction_id,
    GstAmfNode * command_object, GstAmfNode * optional_args)
{
  AmfSerializer _s = { 0 }, *serializer = &_s;

  serializer->size = 4096;
  serializer->data = g_malloc (serializer->size);

  _serialize_u8 (serializer, GST_AMF_TYPE_STRING);
  _serialize_utf8_string (serializer, command_name);
  _serialize_u8 (serializer, GST_AMF_TYPE_NUMBER);
  _serialize_number (serializer, transaction_id);
  _serialize_value (serializer, command_object);
  if (optional_args)
    _serialize_value (serializer, optional_args);

  if (serializer->error) {
    GST_ERROR ("failed to serialize");
    g_free (serializer->data);
    return NULL;
  }
  return g_bytes_new_take (serializer->data, serializer->offset);
}

GBytes *
gst_amf_serialize_command2 (const gchar * command_name, gdouble transaction_id,
    GstAmfNode * command_object, GstAmfNode * optional_args, GstAmfNode * n3,
    GstAmfNode * n4)
{
  AmfSerializer _s = { 0 }, *serializer = &_s;

  serializer->size = 4096;
  serializer->data = g_malloc (serializer->size);

  _serialize_u8 (serializer, GST_AMF_TYPE_STRING);
  _serialize_utf8_string (serializer, command_name);
  _serialize_u8 (serializer, GST_AMF_TYPE_NUMBER);
  _serialize_number (serializer, transaction_id);
  _serialize_value (serializer, command_object);
  if (optional_args)
    _serialize_value (serializer, optional_args);
  if (n3)
    _serialize_value (serializer, n3);
  if (n4)
    _serialize_value (serializer, n4);

  if (serializer->error) {
    GST_ERROR ("failed to serialize");
    g_free (serializer->data);
    return NULL;
  }
  return g_bytes_new_take (serializer->data, serializer->offset);
}

gboolean
gst_amf_node_get_boolean (const GstAmfNode * node)
{
  return node->int_val;
}

const char *
gst_amf_node_get_string (const GstAmfNode * node)
{
  return node->string_val;
}

double
gst_amf_node_get_number (const GstAmfNode * node)
{
  return node->double_val;
}

const GstAmfNode *
gst_amf_node_get_object (const GstAmfNode * node, const gchar * field_name)
{
  guint i;

  if (!node->array_val) {
    GST_WARNING ("tried to access array of non-object node");
    return NULL;
  }

  for (i = 0; i < gst_amf_node_get_object_length (node); i++) {
    AmfObjectField *field = g_ptr_array_index (node->array_val, i);
    if (strcmp (field->name, field_name) == 0) {
      return field->value;
    }
  }
  return NULL;
}

guint
gst_amf_node_get_object_length (const GstAmfNode * node)
{
  return node->array_val->len;
}

const GstAmfNode *
gst_amf_node_get_object_by_index (const GstAmfNode * node, guint i)
{
  AmfObjectField *field;
  field = g_ptr_array_index (node->array_val, i);
  return field->value;
}
