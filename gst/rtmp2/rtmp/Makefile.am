
lib_LTLIBRARIES = libgstrtmp-@GST_API_VERSION@.la

gstrtmp_@GST_API_VERSION@_includedir = $(includedir)/gstreamer-@GST_API_VERSION@/rtmp

libgstrtmp_@GST_API_VERSION@_la_CFLAGS = \
	$(GST_RTMP_CFLAGS) \
	$(GST_CFLAGS) \
	$(SOUP_CFLAGS)
libgstrtmp_@GST_API_VERSION@_la_LIBADD = \
	$(GST_LIBS) \
	$(SOUP_LIBS)
libgstrtmp_@GST_API_VERSION@_la_LDFLAGS = \
	-version-info $(GST_RTMP_LIBVERSION) \
	-no-undefined -export-symbols-regex 'gst_'
libgstrtmp_@GST_API_VERSION@_la_SOURCES = \
	$(sources)

sources = \
	amf.c \
	amf.h \
	rtmpclient.c \
	rtmpclient.h \
	rtmpconnection.c \
	rtmpconnection.h \
	rtmpmessage.c \
	rtmpmessage.h \
	rtmpchunk.c \
	rtmpchunk.h \
	rtmpserver.c \
	rtmpserver.h \
	rtmpstream.c \
	rtmpstream.h \
	rtmputils.c \
	rtmputils.h
