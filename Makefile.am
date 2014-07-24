pkglib_LTLIBRARIES = omkafka.la

omkafka_la_SOURCES = omkafka.c
omkafka_la_CPPFLAGS =  $(RSRT_CFLAGS) $(PTHREADS_CFLAGS)
omkafka_la_LDFLAGS = -module -avoid-version
omkafka_la_LIBADD = -L/tmp/librdkafka/lib -lrdkafka

EXTRA_DIST = 
