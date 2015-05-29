#define CAML_NAME_SPACE

#include <math.h>
#include <errno.h>
#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include <stdlib.h>
#include <signal.h>
#include <wchar.h>

#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/utsname.h>

#ifdef __CYGWIN__
#include <cygwin/socket.h>      /* FIONREAD */
#else
#include <spawn.h>
#endif

#include <regex.h>
#include <stdarg.h>
#include <limits.h>
#include <inttypes.h>

#include <GL/gl.h>

#include <caml/fail.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/unixsupport.h>

#include <ft2build.h>
#include FT_FREETYPE_H
#ifdef USE_FONTCONFIG
#include <fontconfig/fontconfig.h>
#endif

#include <libdjvu/ddjvuapi.h>
#include <libdjvu/miniexp.h>

#include "rune.c"

#ifndef __USE_GNU
extern char **environ;
#endif

#define MIN(a,b) ((a) < (b) ? (a) : (b))
#define MAX(a,b) ((a) > (b) ? (a) : (b))

#if defined __GNUC__
#define NORETURN_ATTR __attribute__ ((noreturn))
#define UNUSED_ATTR __attribute__ ((unused))
#if !defined __clang__
#define OPTIMIZE_ATTR(n) __attribute__ ((optimize ("O"#n)))
#else
#define OPTIMIZE_ATTR(n)
#endif
#define GCC_FMT_ATTR(a, b) __attribute__ ((format (printf, a, b)))
#else
#define NORETURN_ATTR
#define UNUSED_ATTR
#define OPTIMIZE_ATTR(n)
#define GCC_FMT_ATTR(a, b)
#endif

#define FMT_s "zu"

#define FMT_ptr PRIxPTR
#define SCN_ptr SCNxPTR
#define FMT_ptr_cast(p) ((uintptr_t) (p))
#define SCN_ptr_cast(p) ((uintptr_t *) (p))

static void NORETURN_ATTR GCC_FMT_ATTR (2, 3)
    err (int exitcode, const char *fmt, ...)
{
    va_list ap;
    int savederrno;

    savederrno = errno;
    va_start (ap, fmt);
    vfprintf (stderr, fmt, ap);
    va_end (ap);
    fprintf (stderr, ": %s\n", strerror (savederrno));
    fflush (stderr);
    _exit (exitcode);
}

static void NORETURN_ATTR GCC_FMT_ATTR (2, 3)
    errx (int exitcode, const char *fmt, ...)
{
    va_list ap;

    va_start (ap, fmt);
    vfprintf (stderr, fmt, ap);
    va_end (ap);
    fputc ('\n', stderr);
    fflush (stderr);
    _exit (exitcode);
}

#ifndef GL_TEXTURE_RECTANGLE_ARB
#define GL_TEXTURE_RECTANGLE_ARB          0x84F5
#endif

#ifndef GL_BGRA
#define GL_BGRA                           0x80E1
#endif

#ifndef GL_UNSIGNED_INT_8_8_8_8
#define GL_UNSIGNED_INT_8_8_8_8           0x8035
#endif

#ifndef GL_UNSIGNED_INT_8_8_8_8_REV
#define GL_UNSIGNED_INT_8_8_8_8_REV       0x8367
#endif

#if 0
#define lprintf printf
#else
#define lprintf(...)
#endif

#define ARSERT(cond) for (;;) {                         \
    if (!(cond)) {                                      \
        errx (1, "%s:%d " #cond, __FILE__, __LINE__);   \
    }                                                   \
    break;                                              \
}

struct slice {
    int h;
    int texindex;
};

struct tile {
    int w, h, n;
    int slicecount;
    int sliceheight;
    struct pbo *pbo;
    char *pixmap;
    struct slice slices[1];
};

struct pagedim {
    int pageno;
    int rotate;
    int left;
    int w, h;
    struct ddjvu_rect_s bounds;
};

struct page {
    int tgen;
    int sgen;
    int agen;
    int pageno;
    int pdimno;
    struct ddjvu_page_s *djpage;
};

struct {
    int sliceheight;
    struct pagedim *pagedims;
    int pagecount;
    int pagedimcount;
    struct ddjvu_document_s *doc;
    struct ddjvu_context_s *ctx;
    int w, h;
    struct ddjvu_format_s *pixel_format;

    int texindex;
    int texcount;
    GLuint *texids;

    GLenum texiform;
    GLenum texform;
    GLenum texty;

    struct {
        int w, h;
        struct slice *slice;
    } *texowners;

    int rotate;
    enum { FitWidth, FitProportional, FitPage } fitmodel;
    int trimmargins;
    int needoutline;
    int gen;
    int aalevel;

    pthread_t thread;
    int csock;
    FT_Face face;

    int dirty;

    GLuint stid;

    int pbo_usable;
    void (*glBindBufferARB) (GLenum, GLuint);
    GLboolean (*glUnmapBufferARB) (GLenum);
    void *(*glMapBufferARB) (GLenum, GLenum);
    void (*glBufferDataARB) (GLenum, GLsizei, void *, GLenum);
    void (*glGenBuffersARB) (GLsizei, GLuint *);
    void (*glDeleteBuffersARB) (GLsizei, GLuint *);

    GLfloat texcoords[8];
    GLfloat vertices[16];
} state;

struct pbo {
    GLuint id;
    void *ptr;
    size_t size;
};

static void UNUSED_ATTR debug_rect (const char *cap, struct ddjvu_rect_s r)
{
    printf ("%s(rect) %d,%d,%u,%u\n", cap, r.x, r.y, r.w, r.h);
}

static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

static void lock (const char *cap)
{
    int ret = pthread_mutex_lock (&mutex);
    if (ret) {
        errx (1, "%s: pthread_mutex_lock: %s", cap, strerror (ret));
    }
}

static void unlock (const char *cap)
{
    int ret = pthread_mutex_unlock (&mutex);
    if (ret) {
        errx (1, "%s: pthread_mutex_unlock: %s", cap, strerror (ret));
    }
}

static int trylock (const char *cap)
{
    int ret = pthread_mutex_trylock (&mutex);
    if (ret && ret != EBUSY) {
        errx (1, "%s: pthread_mutex_trylock: %s", cap, strerror (ret));
    }
    return ret == EBUSY;
}

static void *parse_pointer (const char *cap, const char *s)
{
    int ret;
    void *ptr;

    ret = sscanf (s, "%" SCN_ptr, SCN_ptr_cast (&ptr));
    if (ret != 1) {
        errx (1, "%s: cannot parse pointer in `%s'", cap, s);
    }
    return ptr;
}

static double now (void)
{
    struct timeval tv;

    if (gettimeofday (&tv, NULL)) {
        err (1, "gettimeofday");
    }
    return tv.tv_sec + tv.tv_usec*1e-6;
}

static void handle_ddjvu_messages (ddjvu_context_t *ctx, int wait)
{
    const ddjvu_message_t *msg;
    if (wait)
        ddjvu_message_wait (ctx);
    while ((msg = ddjvu_message_peek (ctx)))
    {
        switch(msg->m_any.tag) {
        case DDJVU_ERROR: abort (); break;
        case DDJVU_INFO: lprintf ("info\n"); break;
        case DDJVU_NEWSTREAM: lprintf ("newstream\n"); break;
        case DDJVU_DOCINFO: lprintf ("docinfo\n"); break;
        default:
            lprintf ("tag %d\n", msg->m_any.tag);
            break;
        }
        ddjvu_message_pop (ctx);
    }
    /* printf ("done\n"); */
}

static int UNUSED_ATTR hasdata (void)
{
    int ret, avail;
    ret = ioctl (state.csock, FIONREAD, &avail);
    if (ret) err (1, "hasdata: FIONREAD error ret=%d", ret);
    return avail > 0;
}

CAMLprim value ml_hasdata (value fd_v)
{
    CAMLparam1 (fd_v);
    int ret, avail;

    ret = ioctl (Int_val (fd_v), FIONREAD, &avail);
    if (ret) uerror ("ioctl (FIONREAD)", Nothing);
    CAMLreturn (Val_bool (avail > 0));
}

static void readdata (void *p, int size)
{
    ssize_t n;

 again:
    n = read (state.csock, p, size);
    if (n < 0) {
        if (errno == EINTR) goto again;
        err (1, "read (req %d, ret %zd)", size, n);
    }
    if (n - size) {
        if (!n) errx (1, "EOF while reading");
        errx (1, "read (req %d, ret %zd)", size, n);
    }
}

static void writedata (char *p, int size)
{
    ssize_t n;

    p[0] = (size >> 24) & 0xff;
    p[1] = (size >> 16) & 0xff;
    p[2] = (size >>  8) & 0xff;
    p[3] = (size >>  0) & 0xff;

    n = write (state.csock, p, size + 4);
    if (n - size - 4) {
        if (!n) errx (1, "EOF while writing data");
        err (1, "write (req %d, ret %zd)", size + 4, n);
    }
}

static int readlen (void)
{
    unsigned char p[4];

    readdata (p, 4);
    return (p[0] << 24) | (p[1] << 16) | (p[2] << 8) | p[3];
}

static void GCC_FMT_ATTR (1, 2) printd (const char *fmt, ...)
{
    int size = 200, len;
    va_list ap;
    char *buf;

    buf = malloc (size);
    for (;;) {
        if (!buf) err (1, "malloc for temp buf (%d bytes) failed", size);

        va_start (ap, fmt);
        len = vsnprintf (buf + 4, size - 4, fmt, ap);
        va_end (ap);

        if (len > -1) {
            if (len < size - 4) {
                writedata (buf, len);
                break;
            }
            else size = len + 5;
        }
        else {
            err (1, "vsnprintf for `%s' failed", fmt);
        }
        buf = realloc (buf, size);
    }
    free (buf);
}

static void closedoc (void)
{
    if (state.doc) {
        ddjvu_document_release (state.doc);
        state.doc = NULL;
    }
}

static int openxref (char *filename)
{
    int i;

    for (i = 0; i < state.texcount; ++i)  {
        state.texowners[i].w = -1;
        state.texowners[i].slice = NULL;
    }

    closedoc ();

    state.dirty = 0;
    if (state.pagedims) {
        free (state.pagedims);
        state.pagedims = NULL;
    }
    state.pagedimcount = 0;

    state.doc = ddjvu_document_create_by_filename (state.ctx, filename, 0);
    handle_ddjvu_messages (state.ctx, 1);
    state.pagecount = ddjvu_document_get_pagenum (state.doc);
    return 1;
}

static void unlinktile (struct tile *tile)
{
    int i;

    for (i = 0; i < tile->slicecount; ++i) {
        struct slice *s = &tile->slices[i];

        if (s->texindex != -1) {
            if (state.texowners[s->texindex].slice == s) {
                state.texowners[s->texindex].slice = NULL;
            }
        }
    }
}

static void freepage (struct page *page)
{
    if (!page) return;
    ddjvu_page_release (page->djpage);
    free (page);
}

static void freetile (struct tile *tile)
{
    unlinktile (tile);
    if (tile->pbo) {
        free (tile->pbo);
    }
    free (tile->pixmap);
    free (tile);
}

#ifdef __ALTIVEC__
#include <stdint.h>
#include <altivec.h>

static int cacheline32bytes;

static void __attribute__ ((constructor)) clcheck (void)
{
    char **envp = environ;
    unsigned long *auxv;

    while (*envp++);

    for (auxv = (unsigned long *) envp; *auxv != 0; auxv += 2) {
        if (*auxv == 19) {
            cacheline32bytes = auxv[1] == 32;
            return;
        }
    }
}

static void OPTIMIZE_ATTR (3) clearpixmap (fz_pixmap *pixmap)
{
    size_t size = pixmap->w * pixmap->h * pixmap->n;
    if (cacheline32bytes && size > 32) {
        intptr_t a1, a2, diff;
        size_t sizea, i;
        vector unsigned char v = vec_splat_u8 (-1);
        vector unsigned char *p;

        a1 = a2 = (intptr_t) pixmap->samples;
        a2 = (a1 + 31) & ~31;
        diff = a2 - a1;
        sizea = size - diff;
        p = (void *) a2;

        while (a1 != a2) *(char *) a1++ = 0xff;
        for (i = 0; i < (sizea & ~31); i += 32)  {
            __asm volatile ("dcbz %0, %1"::"b"(a2),"r"(i));
            vec_st (v, i, p);
            vec_st (v, i + 16, p);
        }
        while (i < sizea) *((char *) a1 + i++) = 0xff;
    }
    else fz_clear_pixmap_with_value (state.ctx, pixmap, 0xff);
}
#else
#define clearpixmap(p) fz_clear_pixmap_with_value (state.ctx, p, 0xff)
#endif

static void *loadpage (int pageno, int pindex)
{
    struct page *page;

    page = calloc (sizeof (struct page), 1);
    if (!page) {
        err (1, "calloc page %d", pageno);
    }

    page->djpage = ddjvu_page_create_by_pageno (state.doc, pageno);
    page->pdimno = pindex;
    page->pageno = pageno;
    page->sgen = state.gen;
    page->agen = state.gen;
    page->tgen = state.gen;
    return page;
}

static struct tile *alloctile (int h)
{
    int i;
    int slicecount;
    size_t tilesize;
    struct tile *tile;

    slicecount = (h + state.sliceheight - 1) / state.sliceheight;
    tilesize = sizeof (*tile) + ((slicecount - 1) * sizeof (struct slice));
    tile = calloc (tilesize, 1);
    if (!tile) {
        err (1, "can not allocate tile (%" FMT_s " bytes)", tilesize);
    }
    for (i = 0; i < slicecount; ++i) {
        int sh = MIN (h, state.sliceheight);
        tile->slices[i].h = sh;
        tile->slices[i].texindex = -1;
        h -= sh;
    }
    tile->slicecount = slicecount;
    tile->sliceheight = state.sliceheight;
    return tile;
}

static struct tile *rendertile (struct page *page, int x, int y, int w, int h,
                                struct pbo *pbo)
{
    int n = state.texiform == GL_RGB8 ? 3 : 1;
    struct tile *tile;
    struct ddjvu_rect_s rect, UNUSED_ATTR rect1;
    struct pagedim *pdim = state.pagedims + page->pdimno;
    double a UNUSED_ATTR, b UNUSED_ATTR;

    tile = alloctile (h);

    (void) pbo;
    tile->w = w;
    tile->h = h;
    tile->n = n;
    tile->pixmap = malloc (w*h*n);
    if (!tile->pixmap) abort ();
    rect.x = 0;
    rect.y = 0;
    rect.w = pdim->w;
    rect.h = pdim->h;
    rect1.x = x;
    rect1.y = y;
    rect1.w = w;
    rect1.h = h;

    a = now ();
    while (!ddjvu_page_decoding_done (page->djpage)) {
        handle_ddjvu_messages (state.ctx, 1);
    }
    b = now ();
    lprintf ("wait page %d [%d,%d,%d,%d] %d %d %f\n",
             page->pageno, x, y, w, h, rect.w, rect.h, b - a);

    a = now ();
    ddjvu_page_render (page->djpage, DDJVU_RENDER_COLOR, &rect, &rect1,
                       state.pixel_format, w*n, tile->pixmap);
    b = now ();
    lprintf ("page %d [%d,%d,%d,%d] %d %d %f\n",
             page->pageno, x, y, w, h, rect.w, rect.h, b - a);
    return tile;
}

static void initpdims (void)
{
    int i;
    int pw=pw, ph=ph;
    struct pagedim *p = NULL;

    state.pagedimcount = 0;
    for (i = 0; i < state.pagecount; ++i) {
        ddjvu_status_t r;
        struct ddjvu_pageinfo_s info;

        while ((r = ddjvu_document_get_pageinfo (state.doc,
                                                 i, &info)) < DDJVU_JOB_OK)
            handle_ddjvu_messages (state.ctx, 1);
        if (r >= DDJVU_JOB_FAILED)
            abort ();

        if (state.pagedimcount == 0 || info.width != pw || info.height != ph) {
            state.pagedims = p = realloc (state.pagedims,
                                          ++state.pagedimcount * sizeof (*p));
            if (!p) err (1, "failed to realloc pagedims %d",
                         state.pagedimcount);
            p += state.pagedimcount - 1;
            pw = info.width;
            ph = info.height;
            p->left = 0;
            p->pageno = i;
            p->rotate = 0;
            p->bounds.x = 0;
            p->bounds.y = 0;
            p->bounds.w = pw;
            p->bounds.h = ph;
        }
    }
}

static void layout (void)
{
    int i;
    struct pagedim *p = p;
    double maxw = 0.0, zoom = zoom;

    if (!state.pagecount) return;

    switch (state.fitmodel) {
    case FitProportional:
        for (i = 0; i < state.pagedimcount; ++i) {
            double w;

            p = &state.pagedims[i];
            w = p->bounds.w;
            maxw = MAX (w, maxw);
        }
        zoom = state.w / maxw;
        break;

    case FitPage:
        maxw = state.w;
        break;

    case FitWidth:
        break;

    default:
        ARSERT (0 && state.fitmodel);
    }

    for (i = 0; i < state.pagedimcount; ++i) {
        double zw, w;

        p = &state.pagedims[i];
        w = p->bounds.w;

        switch (state.fitmodel) {
        case FitProportional:
            p->left = ((maxw - w) * zoom) / 2.0;
            break;
        case FitPage:
            {
                double zh, h;
                zw = maxw / w;
                h = p->bounds.h;
                zh = state.h / h;
                zoom = MIN (zw, zh);
                p->left = (maxw - (w * zoom)) / 2.0;
            }
            break;
        case FitWidth:
            p->left = 0;
            zoom = state.w / w;
            break;
        }
    }

    do {
        int w, h;
        w = p->bounds.w * zoom;
        h = p->bounds.h * zoom;
        p->w = w;
        p->h = h;
        printd ("pdim %d %d %d %d", p->pageno, w, h, p->left);
    } while (--p >= state.pagedims);
}

static void set_tex_params (int colorspace)
{
    switch (colorspace) {
    case 0:
        state.texiform = GL_RGB8;
        state.texform = GL_RGB;
        state.texty = GL_UNSIGNED_BYTE;
        state.pixel_format = ddjvu_format_create (DDJVU_FORMAT_RGB24, 0, NULL);
        break;
    case 1:
        state.texiform = GL_RGB8;
        state.texform = GL_BGR;
        state.texty = GL_UNSIGNED_BYTE;
        state.pixel_format = ddjvu_format_create (DDJVU_FORMAT_BGR24, 0, NULL);
        break;
    case 2:
        state.texiform = GL_LUMINANCE8;
        state.texform = GL_LUMINANCE;
        state.texty = GL_UNSIGNED_BYTE;
        state.pixel_format = ddjvu_format_create (DDJVU_FORMAT_GREY8, 0, NULL);
        break;
    default:
        errx (1, "invalid colorspce %d", colorspace);
    }
    if (!state.pixel_format) abort ();
    ddjvu_format_set_row_order (state.pixel_format, 1);
    ddjvu_format_set_y_direction (state.pixel_format, 1);
}

static void realloctexts (int texcount)
{
    size_t size;

    if (texcount == state.texcount) return;

    if (texcount < state.texcount) {
        glDeleteTextures (state.texcount - texcount,
                          state.texids + texcount);
    }

    size = texcount * sizeof (*state.texids);
    state.texids = realloc (state.texids, size);
    if (!state.texids) {
        err (1, "realloc texids %" FMT_s, size);
    }

    size = texcount * sizeof (*state.texowners);
    state.texowners = realloc (state.texowners, size);
    if (!state.texowners) {
        err (1, "realloc texowners %" FMT_s, size);
    }
    if (texcount > state.texcount) {
        int i;

        glGenTextures (texcount - state.texcount,
                       state.texids + state.texcount);
        for (i = state.texcount; i < texcount; ++i) {
            state.texowners[i].w = -1;
            state.texowners[i].slice = NULL;
        }
    }
    state.texcount = texcount;
    state.texindex = 0;
}

static char *mbtoutf8 (char *s)
{
    char *p, *r;
    wchar_t *tmp;
    size_t i, ret, len;

    len = mbstowcs (NULL, s, strlen (s));
    if (len == 0) {
        return s;
    }
    else {
        if (len == (size_t) -1)  {
            return s;
        }
    }

    tmp = malloc (len * sizeof (wchar_t));
    if (!tmp) {
        return s;
    }

    ret = mbstowcs (tmp, s, len);
    if (ret == (size_t) -1) {
        free (tmp);
        return s;
    }

    len = 0;
    for (i = 0; i < ret; ++i) {
        len += runelen (tmp[i]);
    }

    p = r = malloc (len + 1);
    if (!r) {
        free (tmp);
        return s;
    }

    for (i = 0; i < ret; ++i) {
        p += runetochar (p, (Rune *) &tmp[i]);
    }
    *p = 0;
    free (tmp);
    return r;
}

CAMLprim value ml_mbtoutf8 (value s_v)
{
    CAMLparam1 (s_v);
    CAMLlocal1 (ret_v);
    char *s, *r;

    s = String_val (s_v);
    r = mbtoutf8 (s);
    if (r == s) {
        ret_v = s_v;
    }
    else {
        ret_v = caml_copy_string (r);
        free (r);
    }
    CAMLreturn (ret_v);
}

static void recurse_outline (struct miniexp_s *e, int level,
                             struct ddjvu_fileinfo_s *info)
{
    int i;
    int fileno = ddjvu_document_get_filenum (state.doc);

    if (e == miniexp_nil) return;

    while (miniexp_consp (e) != 0) {
        struct miniexp_s *inner = miniexp_car (e);
        if (miniexp_consp (inner)
            && miniexp_consp(miniexp_cdr (inner))
            && miniexp_stringp (miniexp_car (inner))
            && miniexp_stringp (miniexp_car (inner))
            ) {
            int pageno;
            const char *name = miniexp_to_str (miniexp_car (inner));
            const char *link = miniexp_to_str (miniexp_cadr (inner));

            if (link == NULL || link[0] != '#') {
                e = miniexp_cdr (e);
                continue;
            }

            pageno = -1;
            for (i = 0; i < fileno; ++i)  {
                if (!strcmp (link+1, info[i].id)) {
                    pageno = info[i].pageno > 0 ? info[i].pageno - 1 : 0;
                    break;
                }
            }
            if (pageno >= 0)
                printd ("o %d %d 1 1 %s", level, pageno, name);
            recurse_outline (miniexp_cddr (inner), level + 1, info);
        }
        e = miniexp_cdr (e);
    }
}

static void process_outline (void)
{
    struct miniexp_s *o;

    if (!state.doc) return;
    while ((o = ddjvu_document_get_outline (state.doc)) == miniexp_dummy) {
        handle_ddjvu_messages (state.ctx, 1);
    }
    if (o == NULL) return;
    if (miniexp_consp (o) == 0
        || miniexp_car (o) != miniexp_symbol ("bookmarks")) {
        ddjvu_miniexp_release (state.doc, o);
        return;
    }
    struct ddjvu_fileinfo_s *info;
    int fileno = ddjvu_document_get_filenum (state.doc);
    info = malloc (sizeof (*info) * fileno);
    if (!info) abort ();;
    for (int i = 0; i < fileno; ++i) {
        ddjvu_document_get_fileinfo (state.doc, i, &info[i]);
    }
    recurse_outline (o, 0, info);
    free (info);
    ddjvu_miniexp_release (state.doc, o);
}

static void * mainloop (void UNUSED_ATTR *unused)
{
    char *p = NULL;
    int len, ret, oldlen = 0;
    int cxack__ = 0;

    for (;;) {
        len = readlen ();
        if (len == 0) {
            errx (1, "readlen returned 0");
        }

        if (oldlen < len + 1) {
            p = realloc (p, len + 1);
            if (!p) {
                err (1, "realloc %d failed", len + 1);
            }
            oldlen = len + 1;
        }
        readdata (p, len);
        p[len] = 0;

        if (!strncmp ("open", p, 4)) {
            int wthack, off, ok = 0;
            char *filename;
            char *utf8filename;

            ret = sscanf (p + 5, " %d %d %n", &wthack, &cxack__, &off);
            if (ret != 2) {
                errx (1, "malformed open `%.*s' ret=%d", len, p, ret);
            }

            filename = p + 5 + off;

            lock ("open");
            openxref (filename);
            if (state.doc) {
                initpdims ();
            }
            unlock ("open");

            if (ok) {
                if (!wthack) {
                    utf8filename = mbtoutf8 (filename);
                    printd ("msg Opened %s (press h/F1 to get help)",
                            utf8filename);
                    if (utf8filename != filename) {
                        free (utf8filename);
                    }
                }
                state.needoutline = 1;
            }
        }
        else if (!strncmp ("cs", p, 2)) {
            int i, colorspace;

            ret = sscanf (p + 2, " %d", &colorspace);
            if (ret != 1) {
                errx (1, "malformed cs `%.*s' ret=%d", len, p, ret);
            }
            lock ("cs");
            set_tex_params (colorspace);
            for (i = 0; i < state.texcount; ++i)  {
                state.texowners[i].w = -1;
                state.texowners[i].slice = NULL;
            }
            unlock ("cs");
        }
        else if (!strncmp ("freepage", p, 8)) {
            void *ptr;

            ret = sscanf (p + 8, " %" SCN_ptr, SCN_ptr_cast (&ptr));
            if (ret != 1) {
                errx (1, "malformed freepage `%.*s' ret=%d", len, p, ret);
            }
            freepage (ptr);
        }
        else if (!strncmp ("freetile", p, 8)) {
            void *ptr;

            ret = sscanf (p + 8, " %" SCN_ptr, SCN_ptr_cast (&ptr));
            if (ret != 1) {
                errx (1, "malformed freetile `%.*s' ret=%d", len, p, ret);
            }
            freetile (ptr);
        }
        else if (!strncmp ("search", p, 6)) {
            abort ();
        }
        else if (!strncmp ("geometry", p, 8)) {
            int w, h, fitmodel;

            printd ("clear");
            ret = sscanf (p + 8, " %d %d %d", &w, &h, &fitmodel);
            if (ret != 3) {
                errx (1, "malformed geometry `%.*s' ret=%d", len, p, ret);
            }

            lock ("geometry");
            state.h = h;
            if (w != state.w) {
                int i;
                state.w = w;
                for (i = 0; i < state.texcount; ++i)  {
                    state.texowners[i].slice = NULL;
                }
            }
            state.fitmodel = fitmodel;
            layout ();
            process_outline ();

            state.gen++;
            unlock ("geometry");
            printd ("continue %d", state.pagecount);
        }
        else if (!strncmp ("reqlayout", p, 9)) {
            int rotate, off, h;
            unsigned int fitmodel;

            printd ("clear");
            ret = sscanf (p + 9, " %d %u %d %n",
                          &rotate, &fitmodel, &h, &off);
            if (ret != 3) {
                errx (1, "bad reqlayout line `%.*s' ret=%d", len, p, ret);
            }
            lock ("reqlayout");
            if (state.rotate != rotate || state.fitmodel != fitmodel) {
                state.gen += 1;
            }
            state.rotate = rotate;
            state.fitmodel = fitmodel;
            state.h = h;
            layout ();
            process_outline ();

            state.gen++;
            unlock ("reqlayout");
            printd ("continue %d", state.pagecount);
        }
        else if (!strncmp ("page", p, 4)) {
            double a, b;
            struct page *page;
            int pageno, pindex;

            ret = sscanf (p + 4, " %d %d", &pageno, &pindex);
            if (ret != 2) {
                errx (1, "bad page line `%.*s' ret=%d", len, p, ret);
            }

            lock ("page");
            a = now ();
            page = loadpage (pageno, pindex);
            b = now ();
            unlock ("page");

            printd ("page %" FMT_ptr " %f", FMT_ptr_cast (page), b - a);
        }
        else if (!strncmp ("tile", p, 4)) {
            int x, y, w, h;
            struct page *page;
            struct tile *tile;
            double a, b;
            void *data;

            ret = sscanf (p + 4, " %" SCN_ptr " %d %d %d %d %" SCN_ptr,
                          SCN_ptr_cast (&page), &x, &y, &w, &h,
                          SCN_ptr_cast (&data));
            if (ret != 6) {
                errx (1, "bad tile line `%.*s' ret=%d", len, p, ret);
            }

            lock ("tile");
            a = now ();
            tile = rendertile (page, x, y, w, h, data);
            b = now ();
            unlock ("tile");

            printd ("tile %d %d %" FMT_ptr " %u %f",
                    x, y,
                    FMT_ptr_cast (tile),
                    tile->w * tile->h * tile->n,
                    b - a);
        }
        else if (!strncmp ("sliceh", p, 6)) {
            int h;

            ret = sscanf (p + 6, " %d", &h);
            if (ret != 1) {
                errx (1, "malformed sliceh `%.*s' ret=%d", len, p, ret);
            }
            if (h != state.sliceheight) {
                int i;

                state.sliceheight = h;
                for (i = 0; i < state.texcount; ++i) {
                    state.texowners[i].w = -1;
                    state.texowners[i].h = -1;
                    state.texowners[i].slice = NULL;
                }
            }
        }
        else if (!strncmp ("interrupt", p, 9)) {
            printd ("vmsg interrupted");
        }
        else if (!strncmp ("trimset", p, 7)) {
        }
        else {
            errx (1, "unknown command %.*s", len, p);
        }
    }
    return 0;
}

CAMLprim value ml_realloctexts (value texcount_v)
{
    CAMLparam1 (texcount_v);
    int ok;

    if (trylock ("ml_realloctexts")) {
        ok = 0;
        goto done;
    }
    realloctexts (Int_val (texcount_v));
    ok = 1;
    unlock ("ml_realloctexts");

 done:
    CAMLreturn (Val_bool (ok));
}

static void UNUSED_ATTR recti (int x0, int y0, int x1, int y1)
{
    GLfloat *v = state.vertices;

    glVertexPointer (2, GL_FLOAT, 0, v);
    v[0] = x0; v[1] = y0;
    v[2] = x1; v[3] = y0;
    v[4] = x0; v[5] = y1;
    v[6] = x1; v[7] = y1;
    glDrawArrays (GL_TRIANGLE_STRIP, 0, 4);
}

#include "glfont.c"

static void uploadslice (struct tile *tile, struct slice *slice)
{
    int offset;
    struct slice *slice1;
    char *texdata;

    offset = 0;
    for (slice1 = tile->slices; slice != slice1; slice1++) {
        offset += slice1->h * tile->w * tile->n;
    }
    if (slice->texindex != -1 && slice->texindex < state.texcount
        && state.texowners[slice->texindex].slice == slice) {
        glBindTexture (GL_TEXTURE_RECTANGLE_ARB, state.texids[slice->texindex]);
    }
    else {
        int subimage = 0;
        int texindex = state.texindex++ % state.texcount;

        if (state.texowners[texindex].w == tile->w) {
            if (state.texowners[texindex].h >= slice->h) {
                subimage = 1;
            }
            else {
                state.texowners[texindex].h = slice->h;
            }
        }
        else  {
            state.texowners[texindex].h = slice->h;
        }

        state.texowners[texindex].w = tile->w;
        state.texowners[texindex].slice = slice;
        slice->texindex = texindex;

        glBindTexture (GL_TEXTURE_RECTANGLE_ARB, state.texids[texindex]);
        if (tile->pbo) {
            state.glBindBufferARB (GL_PIXEL_UNPACK_BUFFER_ARB, tile->pbo->id);
            texdata = 0;
        }
        else {
            texdata = tile->pixmap;
        }
        if (subimage) {
            glTexSubImage2D (GL_TEXTURE_RECTANGLE_ARB,
                             0,
                             0,
                             0,
                             tile->w,
                             slice->h,
                             state.texform,
                             state.texty,
                             texdata+offset
                );
        }
        else {
            glTexImage2D (GL_TEXTURE_RECTANGLE_ARB,
                          0,
                          state.texiform,
                          tile->w,
                          slice->h,
                          0,
                          state.texform,
                          state.texty,
                          texdata+offset
                );
        }
        if (tile->pbo) {
            state.glBindBufferARB (GL_PIXEL_UNPACK_BUFFER_ARB, 0);
        }
    }
}

CAMLprim value ml_begintiles (value unit_v)
{
    CAMLparam1 (unit_v);
    glEnable (GL_TEXTURE_RECTANGLE_ARB);
    glTexCoordPointer (2, GL_FLOAT, 0, state.texcoords);
    glVertexPointer (2, GL_FLOAT, 0, state.vertices);
    CAMLreturn (unit_v);
}

CAMLprim value ml_endtiles (value unit_v)
{
    CAMLparam1 (unit_v);
    glDisable (GL_TEXTURE_RECTANGLE_ARB);
    CAMLreturn (unit_v);
}

CAMLprim value ml_drawtile (value args_v, value ptr_v)
{
    CAMLparam2 (args_v, ptr_v);
    int dispx = Int_val (Field (args_v, 0));
    int dispy = Int_val (Field (args_v, 1));
    int dispw = Int_val (Field (args_v, 2));
    int disph = Int_val (Field (args_v, 3));
    int tilex = Int_val (Field (args_v, 4));
    int tiley = Int_val (Field (args_v, 5));
    char *s = String_val (ptr_v);
    struct tile *tile = parse_pointer ("ml_drawtile", s);
    int slicey, firstslice;
    struct slice *slice;
    GLfloat *texcoords = state.texcoords;
    GLfloat *vertices = state.vertices;

    firstslice = tiley / tile->sliceheight;
    slice = &tile->slices[firstslice];
    slicey = tiley % tile->sliceheight;

    while (disph > 0) {
        int dh;

        dh = slice->h - slicey;
        dh = MIN (disph, dh);
        uploadslice (tile, slice);

        texcoords[0] = tilex;       texcoords[1] = slicey;
        texcoords[2] = tilex+dispw; texcoords[3] = slicey;
        texcoords[4] = tilex;       texcoords[5] = slicey+dh;
        texcoords[6] = tilex+dispw; texcoords[7] = slicey+dh;

        vertices[0] = dispx;        vertices[1] = dispy;
        vertices[2] = dispx+dispw;  vertices[3] = dispy;
        vertices[4] = dispx;        vertices[5] = dispy+dh;
        vertices[6] = dispx+dispw;  vertices[7] = dispy+dh;

        glDrawArrays (GL_TRIANGLE_STRIP, 0, 4);
        dispy += dh;
        disph -= dh;
        slice++;
        ARSERT (!(slice - tile->slices >= tile->slicecount && disph > 0));
        slicey = 0;
    }
    CAMLreturn (Val_unit);
}

CAMLprim value ml_postprocess (value ptr_v, value hlinks_v,
                               value xoff_v, value yoff_v,
                               value li_v)
{
    CAMLparam5 (ptr_v, hlinks_v, xoff_v, yoff_v, li_v);
    CAMLreturn (Val_int (0));
}

CAMLprim value ml_find_page_with_links (value start_page_v, value dir_v)
{
    CAMLparam2 (start_page_v, dir_v);
    CAMLlocal1 (ret_v);

    ret_v = Val_int (0);
    CAMLreturn (ret_v);
}

enum { dir_first, dir_last };
enum { dir_first_visible, dir_left, dir_right, dir_down, dir_up };

CAMLprim value ml_findlink (value ptr_v, value dir_v)
{
    CAMLparam2 (ptr_v, dir_v);
    CAMLlocal1 (ret_v);
    ret_v = Val_int (0);
    CAMLreturn (ret_v);
}

CAMLprim value ml_getlink (value ptr_v, value n_v)
{
    CAMLparam2 (ptr_v, n_v);
    CAMLlocal4 (ret_v, tup_v, str_v, gr_v);
    /* See ml_findlink for caveat */

    ret_v = Val_int (0);
    CAMLreturn (ret_v);
}

CAMLprim value ml_getannotcontents (value ptr_v, value n_v)
{
    CAMLparam2 (ptr_v, n_v);
    CAMLreturn (caml_copy_string (""));
}

CAMLprim value ml_getlinkcount (value ptr_v)
{
    CAMLparam1 (ptr_v);
    CAMLreturn (Val_int (0));
}

CAMLprim value ml_getlinkrect (value ptr_v, value n_v)
{
    CAMLparam2 (ptr_v, n_v);
    CAMLlocal1 (ret_v);

    ret_v = caml_alloc_tuple (4);
    Field (ret_v, 0) = Val_int (0);
    Field (ret_v, 1) = Val_int (0);
    Field (ret_v, 2) = Val_int (0);
    Field (ret_v, 3) = Val_int (0);
    CAMLreturn (ret_v);
}

CAMLprim value ml_whatsunder (value ptr_v, value x_v, value y_v)
{
    CAMLparam3 (ptr_v, x_v, y_v);
    CAMLlocal4 (ret_v, tup_v, str_v, gr_v);
    ret_v = Val_int (0);
    CAMLreturn (ret_v);
}

enum { mark_page, mark_block, mark_line, mark_word };

static int UNUSED_ATTR uninteresting (int c)
{
    return c == ' ' || c == '\n' || c == '\t' || c == '\n' || c == '\r'
        || ispunct (c);
}

CAMLprim value ml_clearmark (value ptr_v)
{
    CAMLparam1 (ptr_v);
    CAMLreturn (Val_unit);
}

CAMLprim value ml_markunder (value ptr_v, value x_v, value y_v, value mark_v)
{
    CAMLparam4 (ptr_v, x_v, y_v, mark_v);
    CAMLlocal1 (ret_v);
    CAMLreturn (ret_v);
}

CAMLprim value ml_rectofblock (value ptr_v, value x_v, value y_v)
{
    CAMLparam3 (ptr_v, x_v, y_v);
    CAMLlocal2 (ret_v, res_v);

    ret_v = Val_int (0);
    CAMLreturn (ret_v);
}

CAMLprim value ml_seltext (value ptr_v, value rect_v)
{
    CAMLparam2 (ptr_v, rect_v);
    CAMLreturn (Val_unit);
}

CAMLprim value ml_popen (value command_v, value fds_v)
{
    CAMLparam2 (command_v, fds_v);
    CAMLlocal2 (l_v, tup_v);
    int ret;
    pid_t pid;
    char *msg = NULL;
    value earg_v = Nothing;
    posix_spawnattr_t attr;
    posix_spawn_file_actions_t fa;
    char *argv[] = { "/bin/sh", "-c", NULL, NULL };

    argv[2] = String_val (command_v);

    if ((ret = posix_spawn_file_actions_init (&fa)) != 0) {
        unix_error (ret, "posix_spawn_file_actions_init", Nothing);
    }

    if ((ret = posix_spawnattr_init (&attr)) != 0) {
        msg = "posix_spawnattr_init";
        goto fail1;
    }

#ifdef POSIX_SPAWN_USEVFORK
    if ((ret = posix_spawnattr_setflags (&attr, POSIX_SPAWN_USEVFORK)) != 0) {
        msg =  "posix_spawnattr_setflags POSIX_SPAWN_USEVFORK";
        goto fail;
    }
#endif

    for (l_v = fds_v; l_v != Val_int (0); l_v = Field (l_v, 1)) {
        int fd1, fd2;

        tup_v = Field (l_v, 0);
        fd1 = Int_val (Field (tup_v, 0));
        fd2 = Int_val (Field (tup_v, 1));
        if (fd2 < 0) {
            if ((ret = posix_spawn_file_actions_addclose (&fa, fd1)) != 0) {
                msg = "posix_spawn_file_actions_addclose";
                earg_v = tup_v;
                goto fail;
            }
        }
        else {
            if ((ret = posix_spawn_file_actions_adddup2 (&fa, fd1, fd2)) != 0) {
                msg = "posix_spawn_file_actions_adddup2";
                earg_v = tup_v;
                goto fail;
            }
        }
    }

    if ((ret = posix_spawn (&pid, "/bin/sh", &fa, &attr, argv, environ))) {
        msg = "posix_spawn";
        goto fail;
    }

 fail:
    if ((ret = posix_spawnattr_destroy (&attr)) != 0) {
        fprintf (stderr, "posix_spawnattr_destroy: %s\n", strerror (ret));
    }

 fail1:
    if ((ret = posix_spawn_file_actions_destroy (&fa)) != 0) {
        fprintf (stderr, "posix_spawn_file_actions_destroy: %s\n",
                 strerror (ret));
    }

    if (msg)
        unix_error (ret, msg, earg_v);

    CAMLreturn (Val_int (pid));
}

CAMLprim value ml_hassel (value ptr_v)
{
    CAMLparam1 (ptr_v);
    CAMLlocal1 (ret_v);
    ret_v = Val_bool (0);
    CAMLreturn (ret_v);
}

CAMLprim value ml_copysel (value fd_v, value ptr_v)
{
    CAMLparam2 (fd_v, ptr_v);
    CAMLreturn (Val_unit);
}

CAMLprim value ml_getpdimrect (value pagedimno_v)
{
    CAMLparam1 (pagedimno_v);
    CAMLlocal1 (ret_v);

    Store_double_field (ret_v, 0, 0.0);
    Store_double_field (ret_v, 1, 0.0);
    Store_double_field (ret_v, 2, 0.0);
    Store_double_field (ret_v, 3, 0.0);

    CAMLreturn (ret_v);
}

CAMLprim value ml_zoom_for_height (value winw_v, value winh_v,
                                   value dw_v, value cols_v)
{
    CAMLparam4 (winw_v, winh_v, dw_v, cols_v);
    CAMLlocal1 (ret_v);
    ret_v = caml_copy_double (1.0);
    CAMLreturn (ret_v);
}

CAMLprim value ml_draw_string (value pt_v, value x_v, value y_v, value string_v)
{
    CAMLparam4 (pt_v, x_v, y_v, string_v);
    CAMLlocal1 (ret_v);
    int pt = Int_val(pt_v);
    int x = Int_val (x_v);
    int y = Int_val (y_v);
    double w;

    w = draw_string (state.face, pt, x, y, String_val (string_v));
    ret_v = caml_copy_double (w);
    CAMLreturn (ret_v);
}

CAMLprim value ml_measure_string (value pt_v, value string_v)
{
    CAMLparam2 (pt_v, string_v);
    CAMLlocal1 (ret_v);
    int pt = Int_val (pt_v);
    double w;

    w = measure_string (state.face, pt, String_val (string_v));
    ret_v = caml_copy_double (w);
    CAMLreturn (ret_v);
}

CAMLprim value ml_getpagebox (value opaque_v)
{
    CAMLparam1 (opaque_v);
    CAMLlocal1 (ret_v);

    ret_v = caml_alloc_tuple (4);
    Field (ret_v, 0) = Val_int (0);
    Field (ret_v, 1) = Val_int (0);
    Field (ret_v, 2) = Val_int (0);
    Field (ret_v, 3) = Val_int (0);

    CAMLreturn (ret_v);
}

CAMLprim value ml_setaalevel (value level_v)
{
    CAMLparam1 (level_v);

    state.aalevel = Int_val (level_v);
    CAMLreturn (Val_unit);
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wvariadic-macros"
#include <X11/Xlib.h>
#pragma GCC diagnostic pop

#include <GL/glx.h>

static struct {
    Window wid;
    Display *dpy;
    GLXContext ctx;
    XVisualInfo *visual;
} glx;

CAMLprim value ml_glxinit (value display_v, value wid_v, value screen_v)
{
    CAMLparam3 (display_v, wid_v, screen_v);
    int attribs[] = {GLX_RGBA,  GLX_DOUBLEBUFFER, None};

    glx.dpy = XOpenDisplay (String_val (display_v));
    if (!glx.dpy) {
        caml_failwith ("XOpenDisplay");
    }

    glx.visual = glXChooseVisual (glx.dpy, Int_val (screen_v), attribs);
    if (!glx.visual) {
        XCloseDisplay (glx.dpy);
        caml_failwith ("glXChooseVisual");
    }

    glx.wid = Int_val (wid_v);
    CAMLreturn (Val_int (glx.visual->visualid));
}

CAMLprim value ml_glxcompleteinit (value unit_v)
{
    CAMLparam1 (unit_v);

    glx.ctx = glXCreateContext (glx.dpy, glx.visual, NULL, True);
    if (!glx.ctx) {
        caml_failwith ("glXCreateContext");
    }

    XFree (glx.visual);
    glx.visual = NULL;

    if (!glXMakeCurrent (glx.dpy, glx.wid, glx.ctx)) {
        glXDestroyContext (glx.dpy, glx.ctx);
        glx.ctx = NULL;
        caml_failwith ("glXMakeCurrent");
    }
    CAMLreturn (Val_unit);
}

CAMLprim value ml_swapb (value unit_v)
{
    CAMLparam1 (unit_v);
    glXSwapBuffers (glx.dpy, glx.wid);
    CAMLreturn (Val_unit);
}

#include "keysym2ucs.c"

CAMLprim value ml_keysymtoutf8 (value keysym_v)
{
    CAMLparam1 (keysym_v);
    CAMLlocal1 (str_v);
    KeySym keysym = Int_val (keysym_v);
    Rune rune;
    int len;
    char buf[5];

    rune = keysym2ucs (keysym);
    len = runetochar (buf, &rune);
    buf[len] = 0;
    str_v = caml_copy_string (buf);
    CAMLreturn (str_v);
}

enum { piunknown, pilinux, piosx, pisun, pibsd, picygwin };

CAMLprim value ml_platform (value unit_v)
{
    CAMLparam1 (unit_v);
    CAMLlocal2 (tup_v, arr_v);
    int platid = piunknown;
    struct utsname buf;

#if defined __linux__
    platid = pilinux;
#elif defined __CYGWIN__
    platid = picygwin;
#elif defined __DragonFly__ || defined __FreeBSD__
    || defined __OpenBSD__ || defined __NetBSD__
    platid = pibsd;
#elif defined __sun__
    platid = pisun;
#elif defined __APPLE__
    platid = piosx;
#endif
    if (uname (&buf)) err (1, "uname");

    tup_v = caml_alloc_tuple (2);
    {
        char const *sar[] = {
            buf.sysname,
            buf.release,
            buf.version,
            buf.machine,
            NULL
        };
        arr_v = caml_copy_string_array (sar);
    }
    Field (tup_v, 0) = Val_int (platid);
    Field (tup_v, 1) = arr_v;
    CAMLreturn (tup_v);
}

CAMLprim value ml_cloexec (value fd_v)
{
    CAMLparam1 (fd_v);
    int fd = Int_val (fd_v);

    if (fcntl (fd, F_SETFD, FD_CLOEXEC, 1)) {
        uerror ("fcntl", Nothing);
    }
    CAMLreturn (Val_unit);
}

CAMLprim value ml_getpbo (value w_v, value h_v, value cs_v)
{
    CAMLparam2 (w_v, h_v);
    CAMLlocal1 (ret_v);
    struct pbo *pbo;
    int w = Int_val (w_v);
    int h = Int_val (h_v);
    int cs = Int_val (cs_v);

    if (state.pbo_usable) {
        pbo = calloc (sizeof (*pbo), 1);
        if (!pbo) {
            err (1, "calloc pbo");
        }

        switch (cs) {
        case 0:
        case 1:
            pbo->size = w*h*4;
            break;
        case 2:
            pbo->size = w*h;
            break;
        default:
            errx (1, "ml_getpbo: invalid colorspace %d", cs);
        }

        state.glGenBuffersARB (1, &pbo->id);
        state.glBindBufferARB (GL_PIXEL_UNPACK_BUFFER_ARB, pbo->id);
        state.glBufferDataARB (GL_PIXEL_UNPACK_BUFFER_ARB, pbo->size,
                               NULL, GL_STREAM_DRAW);
        pbo->ptr = state.glMapBufferARB (GL_PIXEL_UNPACK_BUFFER_ARB,
                                         GL_READ_WRITE);
        state.glBindBufferARB (GL_PIXEL_UNPACK_BUFFER_ARB, 0);
        if (!pbo->ptr) {
            fprintf (stderr, "glMapBufferARB failed: %#x\n", glGetError ());
            state.glDeleteBuffersARB (1, &pbo->id);
            free (pbo);
            ret_v = caml_copy_string ("0");
        }
        else {
            int res;
            char *s;

            res = snprintf (NULL, 0, "%" FMT_ptr, FMT_ptr_cast (pbo));
            if (res < 0) {
                err (1, "snprintf %" FMT_ptr " failed", FMT_ptr_cast (pbo));
            }
            s = malloc (res+1);
            if (!s) {
                err (1, "malloc %d bytes failed", res+1);
            }
            res = sprintf (s, "%" FMT_ptr, FMT_ptr_cast (pbo));
            if (res < 0) {
                err (1, "sprintf %" FMT_ptr " failed", FMT_ptr_cast (pbo));
            }
            ret_v = caml_copy_string (s);
            free (s);
        }
    }
    else {
        ret_v = caml_copy_string ("0");
    }
    CAMLreturn (ret_v);
}

CAMLprim value ml_freepbo (value s_v)
{
    CAMLparam1 (s_v);
    char *s = String_val (s_v);
    struct tile *tile = parse_pointer ("ml_freepbo", s);

    if (tile->pbo) {
        state.glDeleteBuffersARB (1, &tile->pbo->id);
        tile->pbo->id = -1;
        tile->pbo->ptr = NULL;
        tile->pbo->size = -1;
    }
    CAMLreturn (Val_unit);
}

CAMLprim value ml_unmappbo (value s_v)
{
    CAMLparam1 (s_v);
    char *s = String_val (s_v);
    struct tile *tile = parse_pointer ("ml_unmappbo", s);

    if (tile->pbo) {
        state.glBindBufferARB (GL_PIXEL_UNPACK_BUFFER_ARB, tile->pbo->id);
        if (state.glUnmapBufferARB (GL_PIXEL_UNPACK_BUFFER_ARB) == GL_FALSE) {
            errx (1, "glUnmapBufferARB failed: %#x\n", glGetError ());
        }
        tile->pbo->ptr = NULL;
        state.glBindBufferARB (GL_PIXEL_UNPACK_BUFFER_ARB, 0);
    }
    CAMLreturn (Val_unit);
}

static void setuppbo (void)
{
#define GGPA(n) (*(void (**) ()) &state.n = glXGetProcAddress ((GLubyte *) #n))
    state.pbo_usable = GGPA (glBindBufferARB)
        && GGPA (glUnmapBufferARB)
        && GGPA (glMapBufferARB)
        && GGPA (glBufferDataARB)
        && GGPA (glGenBuffersARB)
        && GGPA (glDeleteBuffersARB);
#undef GGPA
}

CAMLprim value ml_pbo_usable (value unit_v)
{
    CAMLparam1 (unit_v);
    CAMLreturn (Val_bool (state.pbo_usable));
}

CAMLprim value ml_unproject (value ptr_v, value x_v, value y_v)
{
    CAMLparam3 (ptr_v, x_v, y_v);
    CAMLlocal2 (ret_v, tup_v);

    ret_v = Val_int (0);
    CAMLreturn (ret_v);
}

CAMLprim value ml_addannot (value ptr_v, value x_v, value y_v,
                            value contents_v)
{
    CAMLparam4 (ptr_v, x_v, y_v, contents_v);
    CAMLreturn (Val_unit);
}

CAMLprim value ml_delannot (value ptr_v, value n_v)
{
    CAMLparam2 (ptr_v, n_v);
    CAMLreturn (Val_unit);
}

CAMLprim value ml_modannot (value ptr_v, value n_v, value str_v)
{
    CAMLparam3 (ptr_v, n_v, str_v);
    CAMLreturn (Val_unit);
}

CAMLprim value ml_hasunsavedchanges (value unit_v)
{
    CAMLparam1 (unit_v);
    CAMLreturn (Val_bool (state.dirty));
}

CAMLprim value ml_savedoc (value path_v)
{
    CAMLparam1 (path_v);
    CAMLreturn (Val_unit);
}

static void makestippletex (void)
{
    const char pixels[] = "\xff\xff\0\0";
    glGenTextures (1, &state.stid);
    glBindTexture (GL_TEXTURE_1D, state.stid);
    glTexParameteri (GL_TEXTURE_1D, GL_TEXTURE_MIN_FILTER, GL_LINEAR);
    glTexParameteri (GL_TEXTURE_1D, GL_TEXTURE_MAG_FILTER, GL_LINEAR);
    glTexImage1D (
        GL_TEXTURE_1D,
        0,
        GL_ALPHA,
        4,
        0,
        GL_ALPHA,
        GL_UNSIGNED_BYTE,
        pixels
        );
}

CAMLprim value ml_fz_version (value UNUSED_ATTR unit_v)
{
    return caml_copy_string (ddjvu_get_version_string ());
}

CAMLprim value ml_init (value csock_v, value params_v)
{
    CAMLparam2 (csock_v, params_v);
    CAMLlocal2 (trim_v, fuzz_v);
    int ret;
    int texcount;
    char *fontpath;
    int colorspace;
    int haspboext;

    state.csock         = Int_val (csock_v);
    state.rotate        = Int_val (Field (params_v, 0));
    state.fitmodel      = Int_val (Field (params_v, 1));
    trim_v              = Field (params_v, 2);
    texcount            = Int_val (Field (params_v, 3));
    state.sliceheight   = Int_val (Field (params_v, 4));
    colorspace          = Int_val (Field (params_v, 6));
    fontpath            = String_val (Field (params_v, 7));

    haspboext           = Bool_val (Field (params_v, 9));

    state.ctx = ddjvu_context_create ("lldp");

    state.trimmargins = Bool_val (Field (trim_v, 0));
    set_tex_params (colorspace);

    if (*fontpath) {
#ifdef FONTCONFIG
        FcChar8 *path;
        FcResult result;
        FcConfig *config;
        char *buf = fontpath;
        FcPattern *pat, *pat1;

        config = FcInitLoadConfigAndFonts ();
        if (!config) {
            errx (1, "FcInitLoadConfigAndFonts failed");
        }

        pat = FcNameParse ((FcChar8 *) buf);
        if (!pat) {
            errx (1, "FcNameParse failed");
        }

        if (!FcConfigSubstitute (config, pat, FcMatchPattern)) {
            errx (1, "FcConfigSubstitute failed");
        }
        FcDefaultSubstitute (pat);

        pat1 = FcFontMatch (config, pat, &result);
        if (!pat1) {
            errx (1, "FcFontMatch failed");
        }

        if (FcPatternGetString (pat1, FC_FILE, 0, &path) != FcResultMatch) {
            errx (1, "FcPatternGetString failed");
        }

        state.face = load_font ((char *) path);
        FcPatternDestroy (pat);
        FcPatternDestroy (pat1);
#else
        state.face = load_font (fontpath);
#endif
    }
    if (!state.face) _exit (1);

    realloctexts (texcount);

    if (haspboext) {
        setuppbo ();
    }

    makestippletex ();

    ret = pthread_create (&state.thread, NULL, mainloop, NULL);
    if (ret) {
        errx (1, "pthread_create: %s", strerror (ret));
    }

    CAMLreturn (Val_unit);
}
