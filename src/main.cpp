// Shopping Sessions pipeline. Rules: rules.md; design: PLAN.md.
// Reads $DATA_DIR, writes $OUTPUT_DIR/{user_daily,campaign_daily}/*.parquet.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>
#include <thread>
#include <vector>
#include <dirent.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#if defined(__AVX2__)
#include <immintrin.h>
#elif defined(__aarch64__)
#include <arm_neon.h>
#endif

using namespace std;
typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef int64_t i64;

static const i64 DAY = 86400000, GAP = 1800000, WEEK = 7 * DAY;
static int PB = 8, NP = 256;  // event-id partitions (bits, count), scaled with input size
static int QB = 8, NQ = 256;  // person partitions

static int T = 8;  // threads
static i64 WS, WE, WSDAY;  // window start/end (ms), first day (days since epoch)
static int ND;             // days in output range

static double t0;
static double now() {
  return chrono::duration<double>(chrono::steady_clock::now().time_since_epoch()).count();
}
static void lg(const char* what) { fprintf(stderr, "[%7.3f] %s\n", now() - t0, what); }

struct SV {
  const char* p;
  u32 n;
  string_view sv() const { return string_view(p, n); }
};

// ---------------------------------------------------------------- parallel
template <class F>
static void par(size_t ntasks, F f) {
  atomic<size_t> next{0};
  vector<thread> th;
  for (int t = 0; t < T; t++)
    th.emplace_back([&, t] {
      for (size_t i; (i = next.fetch_add(1, memory_order_relaxed)) < ntasks;) f(i, t);
    });
  for (auto& x : th) x.join();
}

// ---------------------------------------------------------------- hashing
static inline u64 ld8(const char* p) {
  u64 w;
  memcpy(&w, p, 8);
  return w;
}
static inline u64 mum(u64 a, u64 b) {
  __uint128_t r = (__uint128_t)a * b;
  return (u64)r ^ (u64)(r >> 64);
}
static inline u64 fmix64(u64 x) {  // bijective 64-bit mixer (MurmurHash3 finalizer)
  x ^= x >> 33;
  x *= 0xff51afd7ed558ccdull;
  x ^= x >> 33;
  x *= 0xc4ceb9fe1a85ec53ull;
  return x ^ (x >> 33);
}
// (h1, h2) identifies the string. Up to 15 bytes exactly: h2 = bytes 8..14 | n << 56 and h1 is a
// bijection of bytes 0..7 for a fixed h2. Longer strings: a 128-bit hash with h2's top bit set
// (collision odds ~2^-100 here). h2 is never 0 for n > 0. Callers guarantee 16 readable bytes
// past p (zero-padded mappings and buffers).
static inline void hash128(const char* p, u32 n, u64& h1, u64& h2) {
  if (n <= 15) {
    u64 a = n ? ld8(p) & (~0ull >> (64 - 8 * min(n, 8u))) : 0;
    u64 b = n > 8 ? ld8(p + 8) & (~0ull >> (64 - 8 * (n - 8))) : 0;
    h2 = b | (u64)n << 56;
    h1 = fmix64(a ^ (h2 * 0x9E3779B97F4A7C15ull));
    return;
  }
  u64 a = 0x243F6A8885A308D3ull ^ n, b = 0x13198A2E03707344ull ^ ((u64)n << 32);
  while (n >= 8) {
    u64 w = ld8(p);
    a = mum(a ^ w, 0xa0761d6478bd642full);
    b = mum(b ^ w, 0xe7037ed1a0b428dbull);
    p += 8;
    n -= 8;
  }
  if (n) {
    u64 w = ld8(p) & (~0ull >> (64 - 8 * n));
    a = mum(a ^ w, 0xa0761d6478bd642full);
    b = mum(b ^ w, 0xe7037ed1a0b428dbull);
  }
  h1 = mum(a ^ 0x8ebc6af09c88c6e3ull, 0x589965cc75374cc3ull);
  h2 = mum(b ^ 0x1d8e4e27c47d124full, 0xeb44accab455d165ull) | 1ull << 63;
}

// ---------------------------------------------------------------- CSV dialect
// Splits [s,e) into exactly N fields; false if malformed (count or quoting).
template <int N>
static inline bool split(const char* s, const char* e, SV* f) {
  int n = 0, q = 0;
  const char* a = s;
  for (const char* p = s;; ++p) {
    char c = p == e ? ',' : *p;
    if (c == '"') {
      q++;
      continue;
    }
    if (c != ',') continue;
    if (n == N) return false;
    if (q == 0) {
      f[n].p = a;
      f[n].n = (u32)(p - a);
    } else if (q == 2 && p - a >= 2 && a[0] == '"' && p[-1] == '"') {
      f[n].p = a + 1;
      f[n].n = (u32)(p - a - 2);
    } else
      return false;
    n++;
    q = 0;
    if (p == e) break;
    a = p + 1;
  }
  return n == N;
}

static inline bool dg(char c) { return (unsigned char)(c - '0') < 10; }

static i64 days_from_civil(i64 y, unsigned m, unsigned d) {
  y -= m <= 2;
  const i64 era = (y >= 0 ? y : y - 399) / 400;
  const unsigned yoe = (unsigned)(y - era * 400);
  const unsigned doy = (153 * (m > 2 ? m - 3 : m + 9) + 2) / 5 + d - 1;
  const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + (i64)doe - 719468;
}
static inline i64 floordiv(i64 a, i64 b) { return a >= 0 ? a / b : -((-a + b - 1) / b); }

static int32_t YM_DAYS[400 * 12];  // days since epoch of the 1st of (year 1900..2299, month)
static u8 YM_DIM[400 * 12];       // days in that month
static bool is_leap(int y) { return (y % 4 == 0 && y % 100 != 0) || y % 400 == 0; }
static const int DIM[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
static void init_ym() {
  for (int y = 1900; y < 2300; y++)
    for (int m = 1; m <= 12; m++) {
      int k = (y - 1900) * 12 + m - 1;
      YM_DAYS[k] = (int32_t)days_from_civil(y, m, 1);
      YM_DIM[k] = (u8)(DIM[m - 1] + (m == 2 && is_leap(y)));
    }
}
// all bytes selected by m are ASCII digits
static inline bool swar_digits(u64 w, u64 m) {
  const u64 Z = 0x3030303030303030ull, F = 0xF0F0F0F0F0F0F0F0ull;
  return ((w & F & m) == (Z & m)) && (((w + 0x0606060606060606ull) & F & m) == (Z & m));
}

// 8 ASCII digits (first char most significant) -> value (Lemire's SWAR trick)
static inline u64 eight_digits(u64 w) {
  w -= 0x3030303030303030ull;
  w = (w * 10) + (w >> 8);
  return (((w & 0x000000FF000000FFull) * 0x000F424000000064ull) +
          (((w >> 16) & 0x000000FF000000FFull) * 0x0000271000000001ull)) >> 32;
}

// F1 / F2 / F3 -> epoch ms. false = bad timestamp (out-of-range F3 too).
// Reads up to 16 bytes before p and 8 past p + n: callers pass fields of padded files/buffers
// that start at least 16 bytes in (data lines follow a header).
static bool parse_ts_slow(const char* p, u32 n, i64& out) {  // any year / any F3 length
  if (n == 20 || n == 25) {
    bool f = true;
    for (int k : {0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18}) f &= dg(p[k]);
    if (f && p[4] == '-' && p[7] == '-' && p[10] == 'T' && p[13] == ':' && p[16] == ':') {
      auto D = [&](int k) { return (p[k] - '0') * 10 + (p[k + 1] - '0'); };
      int y = D(0) * 100 + D(2), mo = D(5), d = D(8), h = D(11), mi = D(14), s = D(17);
      if (mo < 1 || mo > 12 || d < 1 || h > 23 || mi > 59 || s > 59) return false;
      if (d > DIM[mo - 1] + (mo == 2 && is_leap(y))) return false;
      i64 off = 0;
      if (n == 20) {
        if (p[19] != 'Z') return false;
      } else {
        char sg = p[19];
        if ((sg != '+' && sg != '-') || !dg(p[20]) || !dg(p[21]) || p[22] != ':' || !dg(p[23]) ||
            !dg(p[24]) || D(20) > 23 || D(23) > 59)
          return false;
        off = (i64)(D(20) * 60 + D(23)) * 60000 * (sg == '-' ? -1 : 1);
      }
      out = (days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + s) * 1000 - off;
      return true;
    }
  }
  if (n == 0) return false;
  for (u32 i = 0; i < n; i++)
    if (!dg(p[i])) return false;
  u32 i = 0;
  while (i < n && p[i] == '0') i++;
  if (n - i > 19) return false;
  u64 v = 0;
  for (; i < n; i++) v = v * 10 + (u64)(p[i] - '0');
  if (v > (u64)INT64_MAX) return false;
  out = (i64)v;
  return true;
}

static inline bool parse_ts(const char* p, u32 n, i64& out) {
  const u64 Z = 0x3030303030303030ull;
  if (n == 20 || n == 25) {
    const u64 M0 = 0x00FFFF00FFFFFFFFull, M1 = 0xFFFF00FFFF00FFFFull;
    u64 w0 = ld8(p), w1 = ld8(p + 8);
    // "YYYY-MM-" "DDTHH:MM" ":SS" then "Z" or "+HH:MM"
    if ((w0 & 0xFF0000FF00000000ull) == 0x2D00002D00000000ull &&
        (w1 & 0x0000FF0000FF0000ull) == 0x00003A0000540000ull && p[16] == ':') {
      u64 v0 = (w0 & M0) - (Z & M0), v1 = (w1 & M1) - (Z & M1);
      auto B = [](u64 v, int k) { return (unsigned)((v >> (8 * k)) & 0xFF); };
      unsigned y = B(v0, 0) * 1000 + B(v0, 1) * 100 + B(v0, 2) * 10 + B(v0, 3);
      unsigned mo = B(v0, 5) * 10 + B(v0, 6);
      unsigned d = B(v1, 0) * 10 + B(v1, 1), h = B(v1, 3) * 10 + B(v1, 4), mi = B(v1, 6) * 10 + B(v1, 7);
      unsigned s = (unsigned)(p[17] - '0') * 10 + (unsigned)(p[18] - '0');
      bool ok = swar_digits(w0, M0) & swar_digits(w1, M1) & dg(p[17]) & dg(p[18]) & (mo - 1 < 12) &
                (d - 1 < 31) & (h < 24) & (mi < 60) & (s < 60);
      i64 off = 0;
      if (n == 20) {
        ok &= p[19] == 'Z';
      } else {
        char sg = p[19];
        unsigned oh = (unsigned)(p[20] - '0') * 10 + (unsigned)(p[21] - '0');
        unsigned om = (unsigned)(p[23] - '0') * 10 + (unsigned)(p[24] - '0');
        ok &= (sg == '+' || sg == '-') & dg(p[20]) & dg(p[21]) & (p[22] == ':') & dg(p[23]) & dg(p[24]) &
              (oh < 24) & (om < 60);
        off = (i64)(oh * 60 + om) * 60000;
        if (sg == '-') off = -off;
      }
      if (!ok) return false;
      if (y - 1900 >= 400) return parse_ts_slow(p, n, out);
      unsigned k = (y - 1900) * 12 + mo - 1;
      if (d > YM_DIM[k]) return false;
      out = ((i64)(YM_DAYS[k] + (int)d - 1) * 86400 + h * 3600 + mi * 60 + s) * 1000 - off;
      return true;
    }
  }
  // F3 of up to 16 digits: right-align into two words of 8 digits, '0'-filled on the left
  if (n == 0 || n > 16) return parse_ts_slow(p, n, out);
  u32 kl = min(n, 8u), kh = n > 8 ? n - 8 : 0;
  u64 ml = ~0ull << (8 * (8 - kl)), mh = kh ? ~0ull << (8 * (8 - kh)) : 0;
  u64 wl = ld8(p + n - 8), wh = ld8(p + (i64)n - 16);
  if (!(swar_digits(wl, ml) & swar_digits(wh, mh))) return false;
  wl = (wl & ml) | (Z & ~ml);
  wh = (wh & mh) | (Z & ~mh);
  out = (i64)(eight_digits(wh) * 100000000 + eight_digits(wl));
  return true;
}

// purchase price: ASCII digits only, 1..10,000,000. Returns 0 if invalid.
static inline u32 parse_price(const char* p, u32 n) {
  if (n == 0) return 0;
  u32 i = 0;
  while (i < n && p[i] == '0') i++;
  u32 v = 0;
  for (u32 k = i; k < n; k++)
    if (!dg(p[k])) return 0;
  if (n - i > 8) return 0;
  for (; i < n; i++) v = v * 10 + (u32)(p[i] - '0');
  return v <= 10000000 ? v : 0;
}

// ---------------------------------------------------------------- files
struct File {
  const char* b;
  size_t n;
};
struct Chunk {
  const char* fb;  // file begin
  const char* fe;  // file end
  const char* b;   // chunk begin
  const char* e;   // chunk end
};

static vector<File> map_dir(const string& dir) {
  vector<File> out;
  DIR* d = opendir(dir.c_str());
  if (!d) return out;
  while (dirent* de = readdir(d)) {
    string name = de->d_name;
    if (name.size() < 4 || name.compare(name.size() - 4, 4, ".csv") != 0 || name[0] == '.') continue;
    string path = dir + "/" + name;
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) continue;
    struct stat st;
    fstat(fd, &st);
    if (!S_ISREG(st.st_mode) || st.st_size == 0) {
      close(fd);
      continue;
    }
    // reserve the file's pages plus one zero page, then map the file over the front: every
    // byte past EOF reads as 0, so scanners and hashes may over-read up to a page
    size_t pg = (size_t)sysconf(_SC_PAGESIZE), span = ((size_t)st.st_size + pg - 1) / pg * pg + pg;
    void* r = mmap(nullptr, span, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    void* m = r == MAP_FAILED ? r : mmap(r, st.st_size, PROT_READ, MAP_PRIVATE | MAP_FIXED, fd, 0);
    close(fd);
    if (m == MAP_FAILED) {
      perror(path.c_str());
      exit(1);
    }
    madvise(m, st.st_size, MADV_SEQUENTIAL);
    out.push_back({(const char*)m, (size_t)st.st_size});
  }
  closedir(d);
  return out;
}

static void make_chunks(const vector<File>& fs, size_t target, vector<Chunk>& out) {
  for (auto& f : fs)
    for (size_t o = 0; o < f.n; o += target)
      out.push_back({f.b, f.b + f.n, f.b + o, f.b + min(f.n, o + target)});
}

// Calls fn(line_begin, line_end) for every non-header line whose first byte is in the chunk.
template <class F>
static inline void for_lines(const Chunk& c, F fn) {
  const char* p = c.b;
  if (p == c.fb) {
    const char* nl = (const char*)memchr(p, '\n', c.fe - p);
    if (!nl) return;
    p = nl + 1;
  } else if (p[-1] != '\n') {
    const char* nl = (const char*)memchr(p, '\n', c.fe - p);
    if (!nl) return;
    p = nl + 1;
  }
  while (p < c.e) {
    const char* nl = (const char*)memchr(p, '\n', c.fe - p);
    const char* le = nl ? nl : c.fe;
    fn(p, le);
    p = le + 1;
  }
}

static string read_file(const string& path) {
  string s;
  FILE* f = fopen(path.c_str(), "rb");
  if (!f) return s;
  char buf[1 << 16];
  size_t k;
  while ((k = fread(buf, 1, sizeof buf, f)) > 0) s.append(buf, k);
  fclose(f);
  s.append(16, '\0');  // keep 16 zero bytes of slack past size(): hash128 over-reads
  s.resize(s.size() - 16);
  return s;
}

// ---------------------------------------------------------------- memory
static void* big_alloc(size_t n) {  // zeroed, lazily committed, hugepage-advised
  const size_t H = 2 << 20;
  n = (n + H - 1) / H * H;
  char* p = (char*)mmap(nullptr, n + H, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
  if (p == MAP_FAILED) {
    perror("mmap");
    exit(1);
  }
  p = (char*)(((uintptr_t)p + H - 1) & ~(uintptr_t)(H - 1));
#ifdef MADV_HUGEPAGE
  madvise(p, n, MADV_HUGEPAGE);
#endif
  return p;
}

// One thread's records split into np partitions: fixed slices of one arena, vector overflow.
template <class R>
struct Parts {
  R* base = nullptr;
  size_t capP = 0, bytes = 0;
  vector<u32> cnt;
  vector<vector<R>> ov;
  void init(int np, size_t cp) {
    capP = cp;
    cnt.assign(np, 0);
    ov.assign(np, {});
    bytes = np * cp * sizeof(R);
    base = (R*)big_alloc(bytes);
  }
  void clear() {
    fill(cnt.begin(), cnt.end(), 0);
    for (auto& v : ov) v.clear();
  }
  inline void push(int p, const R& r) {
    u32 c = cnt[p];
    if (c < capP) {
      base[p * capP + c] = r;
      cnt[p] = c + 1;
    } else
      ov[p].push_back(r);
  }
  size_t size() const {
    size_t n = 0;
    for (size_t p = 0; p < cnt.size(); p++) n += cnt[p] + ov[p].size();
    return n;
  }
  template <class F>
  void each(int p, F f) const {
    const R* b = base + p * capP;
    for (u32 i = 0; i < cnt[p]; i++) f(b[i]);
    for (auto& r : ov[p]) f(r);
  }
};

// ---------------------------------------------------------------- SIMD line scanner
struct Masks {
  u64 comma, quote, nl;
};
#if defined(__AVX2__)
static inline Masks masks64(const char* p) {
  __m256i a = _mm256_loadu_si256((const __m256i*)p), b = _mm256_loadu_si256((const __m256i*)(p + 32));
  auto mm = [&](char c) {
    __m256i v = _mm256_set1_epi8(c);
    return (u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, v)) |
           ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, v)) << 32);
  };
  return {mm(','), mm('"'), mm('\n')};
}
#elif defined(__aarch64__)
static inline u64 nm(uint8x16_t c0, uint8x16_t c1, uint8x16_t c2, uint8x16_t c3) {
  const uint8x16_t bm = {1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128};
  uint8x16_t s0 = vpaddq_u8(vandq_u8(c0, bm), vandq_u8(c1, bm));
  uint8x16_t s1 = vpaddq_u8(vandq_u8(c2, bm), vandq_u8(c3, bm));
  s0 = vpaddq_u8(s0, s1);
  s0 = vpaddq_u8(s0, s0);
  return vgetq_lane_u64(vreinterpretq_u64_u8(s0), 0);
}
static inline Masks masks64(const char* p) {
  const u8* q = (const u8*)p;
  uint8x16_t a = vld1q_u8(q), b = vld1q_u8(q + 16), c = vld1q_u8(q + 32), d = vld1q_u8(q + 48);
  auto mm = [&](u8 ch) {
    uint8x16_t v = vdupq_n_u8(ch);
    return nm(vceqq_u8(a, v), vceqq_u8(b, v), vceqq_u8(c, v), vceqq_u8(d, v));
  };
  return {mm(','), mm('"'), mm('\n')};
}
#else
static inline Masks masks64(const char* p) {
  Masks m{0, 0, 0};
  for (int i = 0; i < 64; i++) {
    m.comma |= (u64)(p[i] == ',') << i;
    m.quote |= (u64)(p[i] == '"') << i;
    m.nl |= (u64)(p[i] == '\n') << i;
  }
  return m;
}
#endif

// Calls fn(fields) for every well-formed N-field line (rules.md "CSV dialect") whose first
// byte lies in the chunk. A file's first line is its header and is skipped. Quotes are checked
// per line: the line is well-formed iff its quote count is twice the number of fields that are
// wrapped in quotes (length >= 2, quote first and last). Every field adds >= 2 resp. >= 0 quotes
// to that count, so equality forces each field to be exactly wrapped or quote-free.
template <int N, class F>
static inline void scan(const Chunk& c, F fn) {
  const char* p = c.b;
  if (p == c.fb || p[-1] != '\n') {
    const char* nl = (const char*)memchr(p, '\n', c.fe - p);
    if (!nl) return;
    p = nl + 1;
  }
  const char* end = c.e;
  if (p >= end) return;
  SV f[N + 1];  // f[N] absorbs extra fields
  int nf = 0;
  const char* fs = p;
  u64 qbase = 0, qline = 0;  // quotes before this block; before this line
  for (const char* B = p;; B += 64) {
    Masks m = masks64(B);  // may read into the zero page after the file
    if (B + 64 > c.fe) m.nl |= 1ull << (c.fe - B);  // the file's end ends its last line
    for (u64 st = m.comma | m.nl; st; st &= st - 1) {
      int bit = __builtin_ctzll(st);
      const char* x = B + bit;
      f[nf < N ? nf : N] = {fs, (u32)(x - fs)};
      nf++;
      fs = x + 1;
      if ((m.nl >> bit) & 1) {
        u64 q = qbase + (u64)__builtin_popcountll(m.quote & ((1ull << bit) - 1));
        u64 nq = q - qline;
        qline = q;
        if (nf == N) {
          u64 w = 0;
          for (int k = 0; k < N; k++) {
            const char* a = f[k].p;
            u32 n = f[k].n;
            u32 wr = (n >= 2) & (a[0] == '"') & (a[(i64)n - 1] == '"');  // n == 0: reads separators
            w += wr;
            f[k].p = a + wr;
            f[k].n = n - 2 * wr;
          }
          if (nq == 2 * w) fn(f);
        }
        nf = 0;
        if (fs >= end) return;
      }
    }
    qbase += (u64)__builtin_popcountll(m.quote);
  }
}

// ---------------------------------------------------------------- small lookup (products, campaigns)
struct SmallMap {  // keys are views into a file buffer that outlives the map
  vector<SV> keys;
  vector<int> val, slot;
  u64 mask = 0;
  void add_all(const vector<pair<SV, int>>& kv) {
    size_t cap = 16;
    while (cap < kv.size() * 2) cap <<= 1;
    mask = cap - 1;
    slot.assign(cap, -1);
    for (auto& [k, v] : kv)
      if (find_slot(k.p, k.n) < 0) {
        u64 h1, h2;
        hash128(k.p, k.n, h1, h2);
        size_t i = h1 & mask;
        while (slot[i] >= 0) i = (i + 1) & mask;
        slot[i] = (int)keys.size();
        keys.push_back(k);
        val.push_back(v);
      }
  }
  int find_slot(const char* p, u32 n) const {
    u64 h1, h2;
    hash128(p, n, h1, h2);
    for (size_t i = h1 & mask; slot[i] >= 0; i = (i + 1) & mask) {
      const SV& k = keys[slot[i]];
      if (k.n == n && memcmp(k.p, p, n) == 0) return slot[i];
    }
    return -1;
  }
  int get(const char* p, u32 n) const {
    int k = find_slot(p, n);
    return k < 0 ? -1 : val[k];
  }
};

// ---------------------------------------------------------------- user dictionary (lock-free insert)
struct Slot {  // 32 B; slot index = uid
  atomic<u64> b;  // h2, claimed by CAS (never 0)
  atomic<u64> a;  // h1
  const char* ptr;
  atomic<u32> len;     // written last: 0 while the inserter is still filling the slot
  atomic<u32> rootp1;  // linked IDs: slot of the person's name + 1; 0 = own person (union-find parent first)
};
struct UDict {
  Slot* s = nullptr;
  u64 mask = 0, cap = 0;
  atomic<u64> count{0};
  atomic<bool> overflow{false};
  void init(u64 c) {
    cap = c;
    mask = c - 1;
    s = (Slot*)big_alloc(c * sizeof(Slot));
    count = 0;
    overflow = false;
  }
  void release() { munmap(s, cap * sizeof(Slot)); }
  inline void prefetch(u64 h1) const { __builtin_prefetch(&s[h1 & mask]); }
  // slot of the ID (n > 0), inserting it if new; `ins` batches this thread's insert count
  inline u32 get(u64 h1, u64 h2, const char* p, u32 n, u64& ins) {
    for (u64 i = h1 & mask, probes = 0;; i = (i + 1) & mask) {
      if (++probes > cap) {  // full: the caller retries with a bigger table
        overflow = true;
        return 0;
      }
      u64 cur = s[i].b.load(memory_order_acquire);
      if (cur == 0) {
        u64 exp = 0;
        if (s[i].b.compare_exchange_strong(exp, h2, memory_order_acq_rel)) {
          s[i].a.store(h1, memory_order_relaxed);
          s[i].ptr = p;
          s[i].len.store(n, memory_order_release);
          if (++ins == 256) {
            ins = 0;
            if (count.fetch_add(256) + 256 > cap / 10 * 7) overflow = true;
          }
          return (u32)i;
        }
        cur = exp;
      }
      if (cur == h2) {
        while (s[i].len.load(memory_order_acquire) == 0) {
        }
        if (s[i].a.load(memory_order_relaxed) == h1) return (u32)i;
      }
    }
  }
  inline u32 get(const char* p, u32 n, u64& ins) {
    u64 h1, h2;
    hash128(p, n, h1, h2);
    return get(h1, h2, p, n, ins);
  }
  inline u32 person(u32 slot) const {
    u32 r = s[slot].rootp1.load(memory_order_relaxed);
    return r ? r - 1 : slot;
  }
  string_view str(u32 slot) const { return string_view(s[slot].ptr, s[slot].len.load(memory_order_relaxed)); }
};
static UDict U;

// Lock-free union-find over slots (parent + 1 in rootp1, 0 = root). Roots are linked by CAS,
// always the larger index under the smaller, so parents < children and no cycle can form.
static inline u32 uf_find(Slot* S, u32 x) {
  for (;;) {
    u32 p1 = S[x].rootp1.load(memory_order_relaxed);
    if (!p1) return x;
    u32 g1 = S[p1 - 1].rootp1.load(memory_order_relaxed);
    if (!g1) return p1 - 1;
    S[x].rootp1.store(g1, memory_order_relaxed);  // path halving: g is still an ancestor
    x = g1 - 1;
  }
}
static inline void uf_unite(Slot* S, u32 a, u32 b) {
  for (;;) {
    a = uf_find(S, a);
    b = uf_find(S, b);
    if (a == b) return;
    if (a < b) swap(a, b);
    u32 root = 0;
    if (S[a].rootp1.compare_exchange_weak(root, b + 1, memory_order_relaxed)) return;
  }
}

// ---------------------------------------------------------------- records
struct ERec {  // one parsed event copy (32 B), partitioned by event_id hash
  u64 h1, h2;
  u32 ingest;  // seconds, offset so the window sits mid-range (ingest_ts is whole-second F1)
  u32 person;
  u32 ts;  // ms since WS
  u32 pc;  // price (bits 0-23; 0 for view/cart) | category << 24 | valid << 31
};
struct PRec {  // surviving event, partitioned by person
  u64 key;     // person << 32 | ts
  u32 price;
  u32 cat;
};
struct CRec {  // valid click, partitioned by person
  u64 key;     // person << 32 | (ts - (WS - WEEK))
  const char* cid;
  u32 clen;
  u32 camp;
};

struct TL {
  Parts<ERec> ep;  // by event-id hash
  Parts<CRec> cp;  // by person
  Parts<PRec> pp;  // by person
  vector<u64> ev64, ck64, rtmp;
  vector<u32> pers;
  vector<CRec> ckbuf;
  vector<pair<u32, u32>> links;
  double tm[3] = {0, 0, 0};
  vector<u32> lroots;
  vector<ERec> dtab;                      // dedup table
  vector<u64> ltab;                       // person -> local id (sessions phase)
  vector<u32> lid, est_, cst_, epos, cpos;
  u64 ins = 0;
  // outputs
  vector<u32> o_person;
  vector<int32_t> o_day;
  vector<i64> o_sess, o_ev, o_rev;
  vector<int> o_top;
  vector<i64> c_sess, c_purch, c_rev, c_assist;  // [camp * ND + day]
};
static vector<TL> tls;

// Sorts n u64 keys by bits [lo, lo + 33): insertion sort when small, LSD radix when big.
static void sort_u64(u64* a, size_t n, int lo, vector<u64>& tmp) {
  if (n < 2) return;
  if (n <= 32) {
    for (size_t x = 1; x < n; x++) {
      u64 v = a[x];
      size_t y = x;
      for (; y > 0 && v < a[y - 1]; y--) a[y] = a[y - 1];
      a[y] = v;
    }
    return;
  }
  if (n <= 2048) {
    sort(a, a + n);
    return;
  }
  tmp.resize(n);
  u64 *src = a, *dst = tmp.data();
  static thread_local u32 cnt[2048];
  for (int sh = lo; sh < lo + 33; sh += 11) {
    memset(cnt, 0, sizeof cnt);
    for (size_t i = 0; i < n; i++) cnt[(src[i] >> sh) & 2047]++;
    for (u32 b = 0, sum = 0; b < 2048; b++) {
      u32 c = cnt[b];
      cnt[b] = sum;
      sum += c;
    }
    for (size_t i = 0; i < n; i++) dst[cnt[(src[i] >> sh) & 2047]++] = src[i];
    swap(src, dst);
  }
  if (src != a) memcpy(a, src, n * sizeof(u64));
}

static inline int qpart(u32 person) { return (int)((person * 0x9E3779B1u) >> (32 - QB)); }
static inline int ppart(u64 h1) { return (int)(h1 >> (64 - PB)); }

// ---------------------------------------------------------------- parquet writer
struct Buf {
  string s;
  void b(u8 c) { s.push_back((char)c); }
  void var(u64 v) {
    while (v >= 0x80) {
      b((u8)(v | 0x80));
      v >>= 7;
    }
    b((u8)v);
  }
  void zz(i64 v) { var(((u64)v << 1) ^ (u64)(v >> 63)); }
  void raw(const void* p, size_t n) { s.append((const char*)p, n); }
  void u32le(u32 v) { raw(&v, 4); }
};
struct Thrift {  // compact protocol writer
  Buf& o;
  int last[16] = {0};
  int d = 0;
  explicit Thrift(Buf& b) : o(b) {}
  void fh(int id, int type) {
    int delta = id - last[d];
    if (delta > 0 && delta <= 15)
      o.b((u8)((delta << 4) | type));
    else {
      o.b((u8)type);
      o.zz(id);
    }
    last[d] = id;
  }
  void i32(int id, i64 v) {
    fh(id, 5);
    o.zz(v);
  }
  void i64f(int id, i64 v) {
    fh(id, 6);
    o.zz(v);
  }
  void str(int id, string_view v) {
    fh(id, 8);
    o.var(v.size());
    o.raw(v.data(), v.size());
  }
  void list(int id, int etype, size_t n) {
    fh(id, 9);
    if (n < 15)
      o.b((u8)((n << 4) | etype));
    else {
      o.b((u8)(0xF0 | etype));
      o.var(n);
    }
  }
  void sbegin(int id) {  // struct field
    fh(id, 12);
    last[++d] = 0;
  }
  void ebegin() { last[++d] = 0; }  // struct element of a list
  void end() {
    o.b(0);
    d--;
  }
};

enum { PQ_INT32 = 1, PQ_INT64 = 2, PQ_BYTES = 6 };
struct PCol {
  string name;
  int type;
  bool optional;
  int conv;  // -1 none, 0 UTF8, 6 DATE
  Buf data;  // PLAIN values (non-null only)
  vector<u8> def;
};

static void write_parquet(const string& path, vector<PCol>& cols, size_t rows) {
  FILE* fp = fopen(path.c_str(), "wb");
  if (!fp) {
    perror(path.c_str());
    exit(1);
  }
  i64 pos = 0;
  auto put = [&](const void* p, size_t n) {
    fwrite(p, 1, n, fp);
    pos += (i64)n;
  };
  put("PAR1", 4);
  struct Meta {
    i64 off, size, nvals;
  };
  vector<Meta> meta;
  for (auto& c : cols) {
    Buf lv;  // definition levels: 4-byte length + RLE runs, bit width 1
    if (c.optional) {
      Buf rle;
      for (size_t i = 0; i < c.def.size();) {
        size_t j = i;
        while (j < c.def.size() && c.def[j] == c.def[i]) j++;
        rle.var((u64)(j - i) << 1);
        rle.b(c.def[i]);
        i = j;
      }
      lv.u32le((u32)rle.s.size());
      lv.raw(rle.s.data(), rle.s.size());
    }
    i64 psize = (i64)(lv.s.size() + c.data.s.size());
    Buf hdr;
    Thrift t(hdr);
    t.i32(1, 0);  // DATA_PAGE
    t.i32(2, psize);
    t.i32(3, psize);
    t.sbegin(5);
    t.i32(1, (i64)rows);
    t.i32(2, 0);  // PLAIN
    t.i32(3, 3);  // RLE
    t.i32(4, 3);  // RLE
    t.end();
    hdr.b(0);
    meta.push_back({pos, (i64)hdr.s.size() + psize, (i64)rows});
    put(hdr.s.data(), hdr.s.size());
    put(lv.s.data(), lv.s.size());
    put(c.data.s.data(), c.data.s.size());
  }
  Buf m;
  Thrift t(m);
  t.i32(1, 1);
  t.list(2, 12, cols.size() + 1);
  t.ebegin();
  t.str(4, "schema");
  t.i32(5, (i64)cols.size());
  t.end();
  for (auto& c : cols) {
    t.ebegin();
    t.i32(1, c.type);
    t.i32(3, c.optional ? 1 : 0);
    t.str(4, c.name);
    if (c.conv >= 0) {
      t.i32(6, c.conv);
      t.sbegin(10);  // LogicalType union
      t.sbegin(c.conv == 0 ? 1 : 6);
      t.end();
      t.end();
    }
    t.end();
  }
  t.i64f(3, (i64)rows);
  i64 total = 0;
  for (auto& x : meta) total += x.size;
  t.list(4, 12, rows ? 1 : 0);
  if (rows) {
    t.ebegin();
    t.list(1, 12, cols.size());
    for (size_t k = 0; k < cols.size(); k++) {
      t.ebegin();
      t.i64f(2, meta[k].off);
      t.sbegin(3);
      t.i32(1, cols[k].type);
      t.list(2, 5, 2);
      m.zz(0);
      m.zz(3);
      t.list(3, 8, 1);
      m.var(cols[k].name.size());
      m.raw(cols[k].name.data(), cols[k].name.size());
      t.i32(4, 0);
      t.i64f(5, meta[k].nvals);
      t.i64f(6, meta[k].size);
      t.i64f(7, meta[k].size);
      t.i64f(9, meta[k].off);
      t.end();
      t.end();
    }
    t.i64f(2, total);
    t.i64f(3, (i64)rows);
    t.end();
  }
  m.b(0);
  m.u32le((u32)m.s.size());  // footer length: the metadata alone
  m.raw("PAR1", 4);
  put(m.s.data(), m.s.size());
  if (fclose(fp) != 0) {
    perror(path.c_str());
    exit(1);
  }
}

static void put_i64(PCol& c, i64 v) { c.data.raw(&v, 8); }
static void put_i32(PCol& c, int32_t v) { c.data.raw(&v, 4); }
static void put_str(PCol& c, string_view s) {
  c.data.u32le((u32)s.size());
  c.data.raw(s.data(), s.size());
}

// ---------------------------------------------------------------- main
int main() {
  t0 = now();
  init_ym();
  const char* e;
  string DATA = (e = getenv("DATA_DIR")) ? e : "/data";
  string OUT = (e = getenv("OUTPUT_DIR")) ? e : "/output";
  T = (e = getenv("NCPU")) ? atoi(e) : (int)thread::hardware_concurrency();
  if (T < 1) T = 1;
  tls.resize(T);

  // manifest
  {
    string m = read_file(DATA + "/manifest.json");
    auto grab = [&](const char* key) {
      size_t k = m.find(key);
      size_t a = m.find('"', m.find(':', k) + 1) + 1, b = m.find('"', a);
      i64 v = 0;
      if (k == string::npos || !parse_ts(m.data() + a, (u32)(b - a), v)) {
        fprintf(stderr, "bad manifest\n");
        exit(1);
      }
      return v;
    };
    WS = grab("\"window_start\"");
    WE = grab("\"window_end\"");
    WSDAY = floordiv(WS, DAY);
    ND = (int)(floordiv(WE - 1, DAY) - WSDAY + 1);
  }

  lg("manifest");
  // products -> category index (categories sorted byte-wise, "unknown" among them)
  SmallMap prod, camp;
  vector<SV> cats, chans;
  int CAT_UNKNOWN;
  // products/campaigns are first needed by the events phase: load them alongside links
  thread small_files([&] {
  static string prod_file = read_file(DATA + "/products.csv"), camp_file = read_file(DATA + "/campaigns.csv");
  {
    vector<pair<SV, SV>> rows;
    Chunk c{prod_file.data(), prod_file.data() + prod_file.size(), prod_file.data(),
            prod_file.data() + prod_file.size()};
    for_lines(c, [&](const char* a, const char* b) {
      SV x[2];
      if (split<2>(a, b, x)) rows.push_back({x[0], x[1]});
    });
    static const char UNK[] = "unknown";
    vector<SV> idx_cat;  // row category -> first-seen category slot
    auto cat_of = [&](SV v) {
      for (size_t k = 0; k < cats.size(); k++)
        if (cats[k].sv() == v.sv()) return (int)k;
      cats.push_back(v);
      return (int)cats.size() - 1;
    };
    cat_of(SV{UNK, 7});
    vector<int> first(rows.size());
    for (size_t r = 0; r < rows.size(); r++) {
      first[r] = cat_of(rows[r].second);
      if (cats.size() > 127) {  // ERec packs the category in 7 bits
        fprintf(stderr, "too many categories\n");
        exit(1);
      }
    }
    vector<int> order(cats.size()), rank(cats.size());
    for (size_t k = 0; k < order.size(); k++) order[k] = (int)k;
    sort(order.begin(), order.end(), [&](int a, int b) { return cats[a].sv() < cats[b].sv(); });
    for (size_t k = 0; k < order.size(); k++) rank[order[k]] = (int)k;
    vector<SV> sorted(cats.size());
    for (size_t k = 0; k < cats.size(); k++) sorted[rank[k]] = cats[k];
    CAT_UNKNOWN = rank[0];
    cats = sorted;
    vector<pair<SV, int>> kv(rows.size());
    for (size_t r = 0; r < rows.size(); r++) kv[r] = {rows[r].first, rank[first[r]]};
    prod.add_all(kv);
  }
  {
    Chunk c{camp_file.data(), camp_file.data() + camp_file.size(), camp_file.data(),
            camp_file.data() + camp_file.size()};
    vector<pair<SV, int>> kv;
    for_lines(c, [&](const char* a, const char* b) {
      SV x[2];
      if (split<2>(a, b, x)) {
        kv.push_back({x[0], (int)chans.size()});
        chans.push_back(x[1]);
      }
    });
    camp.add_all(kv);
  }
    lg("small files");
  });

  vector<File> evf = map_dir(DATA + "/events"), ckf = map_dir(DATA + "/clicks"),
               lkf = map_dir(DATA + "/links");
  size_t evb = 0, ckb = 0, lkb = 0;
  for (auto& f : evf) evb += f.n;
  for (auto& f : ckf) ckb += f.n;
  for (auto& f : lkf) lkb += f.n;
  while (PB < 10 && (evb / 85 >> PB) > 80000) PB++;  // ~80k events per partition
  QB = PB;
  NP = 1 << PB;
  NQ = 1 << QB;
  vector<Chunk> lch, ech;
  const size_t CH = 4 << 20;
  make_chunks(lkf, CH, lch);
  make_chunks(evf, CH, ech);
  size_t n_ev_chunks = ech.size();
  make_chunks(ckf, CH, ech);  // events then clicks in one work list
#ifdef __linux__  // async readahead on Linux (synchronous on macOS)
  for (auto* fs : {&lkf, &evf, &ckf})
    for (auto& f : *fs) madvise((void*)f.b, f.n, MADV_WILLNEED);
#endif
  lg("mapped");

  // dictionary sized from the input: generator data has ~0.6x this estimate of distinct IDs;
  // a bigger true count only costs a rerun (overflow check at 70% load)
  u64 est = evb / 1000 + ckb / 4000 + lkb / 30 + 1024, cap = 1 << 16;
  while (cap < est * 3 / 2) cap <<= 1;
  {
    size_t evl = evb / 75 + 1024, ckl = ckb / 45 + 1024;
    for (auto& L : tls) {
      L.ep.init(NP, evl / T / NP * 5 / 4 + 256);
      L.cp.init(NQ, ckl / T / NQ * 5 / 4 + 256);
      L.pp.init(NQ, evl / T / NQ * 3 / 2 + 256);
    }
  }
  lg("alloc");

  for (;;) {  // retried with a bigger dictionary on overflow
    U.init(cap);
    for (auto& L : tls) {
      L.links.clear();
      L.ins = 0;
      L.ep.clear();
      L.cp.clear();
    }
    // ---- links
    par(lch.size(), [&](size_t i, int t) {
      TL& L = tls[t];
      scan<2>(lch[i], [&](const SV* x) {
        if (!x[0].n || !x[1].n) return;
        u32 a = U.get(x[0].p, x[0].n, L.ins), b = U.get(x[1].p, x[1].n, L.ins);
        L.links.push_back({a, b});
        uf_unite(U.s, a, b);
      });
    });
    if (U.overflow) {
      U.release();
      cap <<= 2;
      continue;
    }
    lg("links parsed");
    {
      Slot* S = U.s;
      // the person's name is its byte-wise smallest ID; point every linked ID at that slot
      atomic<u32>* best = (atomic<u32>*)big_alloc(cap * 4);
      par(T, [&](size_t w, int) {
        auto& lk = tls[w].links;
        auto& rt = tls[w].lroots;
        rt.resize(lk.size() * 2);
        for (size_t k = 0; k < lk.size(); k++) {
          u32 e2[2] = {lk[k].first, lk[k].second};
          for (int h = 0; h < 2; h++) {
            u32 x = e2[h], r = uf_find(S, x);
            rt[2 * k + h] = r;
            u32 cur = best[r].load(memory_order_relaxed);
            for (;;) {
              if (cur && !(U.str(x) < U.str(cur - 1))) break;
              if (best[r].compare_exchange_weak(cur, x + 1)) break;
            }
          }
        }
      });
      par(T, [&](size_t w, int) {
        auto& lk = tls[w].links;
        auto& rt = tls[w].lroots;
        for (size_t k = 0; k < lk.size(); k++) {
          u32 e2[2] = {lk[k].first, lk[k].second};
          for (int h = 0; h < 2; h++) {
            u32 b = best[rt[2 * k + h]].load(memory_order_relaxed);
            S[e2[h]].rootp1.store(b == e2[h] + 1 ? 0 : b, memory_order_relaxed);
          }
        }
      });
      munmap(best, cap * 4);
    }
    lg("links");

    if (small_files.joinable()) small_files.join();
    // ---- events + clicks; user lookups are prefetched RING records ahead
    const i64 CB = WS - WEEK;  // click ts base
    enum { RING = 16 };
    par(ech.size(), [&](size_t i, int t) {
      TL& L = tls[t];
      if (U.overflow) return;
      if (i < n_ev_chunks) {
        struct Pend {
          ERec r;
          u64 h1, h2;
          const char* p;
          u32 n;
        } ring[RING];
        size_t ri = 0;
        auto resolve = [&](Pend& x) {
          x.r.person = U.person(U.get(x.h1, x.h2, x.p, x.n, L.ins));
          L.ep.push(ppart(x.r.h1), x.r);
        };
        scan<7>(ech[i], [&](const SV* x) {
          if (!x[0].n) return;
          ERec r;
          hash128(x[0].p, x[0].n, r.h1, r.h2);
          i64 ing;
          bool ing_ok = parse_ts(x[3].p, x[3].n, ing);
          // shortcut: clamps ingest beyond +-68 years of the window, which F1 data never has
          r.ingest = ing_ok ? (u32)max<i64>(0, min<i64>(UINT32_MAX - 1, floordiv(ing, 1000) - WS / 1000 + (1ll << 31)))
                            : UINT32_MAX;
          r.person = 0;
          r.ts = 0;
          r.pc = 0;
          i64 ts;
          do {
            if (!x[1].n || !x[5].n || !ing_ok) break;
            if (!parse_ts(x[2].p, x[2].n, ts) || ts < WS || ts >= WE) break;
            if (ing - ts > DAY) break;
            const SV& ty = x[4];
            if (ty.n == 8 && memcmp(ty.p, "purchase", 8) == 0) {
              u32 price = parse_price(x[6].p, x[6].n);
              if (!price) break;
              int c = prod.get(x[5].p, x[5].n);
              r.pc = price | (u32)(c < 0 ? CAT_UNKNOWN : c) << 24;
            } else if (!(ty.n == 4 && (memcmp(ty.p, "view", 4) == 0 || memcmp(ty.p, "cart", 4) == 0)))
              break;
            r.pc |= 1u << 31;
            r.ts = (u32)(ts - WS);
            Pend& slot = ring[ri % RING];
            if (ri >= RING) resolve(slot);
            slot.r = r;
            hash128(x[1].p, x[1].n, slot.h1, slot.h2);
            slot.p = x[1].p;
            slot.n = x[1].n;
            U.prefetch(slot.h1);
            ri++;
            return;
          } while (0);
          L.ep.push(ppart(r.h1), r);
        });
        for (size_t k = ri > RING ? ri - RING : 0; k < ri; k++) resolve(ring[k % RING]);
      } else {
        struct Pend {
          CRec r;
          u64 h1, h2;
          const char* p;
          u32 n;
        } ring[RING];
        size_t ri = 0;
        auto resolve = [&](Pend& x) {
          u32 person = U.person(U.get(x.h1, x.h2, x.p, x.n, L.ins));
          x.r.key |= (u64)person << 32;
          L.cp.push(qpart(person), x.r);
        };
        scan<4>(ech[i], [&](const SV* x) {
          if (!x[0].n || !x[1].n) return;
          i64 ts;
          if (!parse_ts(x[3].p, x[3].n, ts) || ts < CB || ts >= WE) return;
          int c = camp.get(x[2].p, x[2].n);
          if (c < 0) return;
          Pend& slot = ring[ri % RING];
          if (ri >= RING) resolve(slot);
          slot.r = {(u64)(ts - CB), x[0].p, x[0].n, (u32)c};
          hash128(x[1].p, x[1].n, slot.h1, slot.h2);
          slot.p = x[1].p;
          slot.n = x[1].n;
          U.prefetch(slot.h1);
          ri++;
        });
        for (size_t k = ri > RING ? ri - RING : 0; k < ri; k++) resolve(ring[k % RING]);
      }
    });
    if (U.overflow) {
      U.release();
      cap <<= 2;
      continue;
    }
    break;
  }
  {
    size_t ne = 0, nc = 0, nl = 0;
    for (auto& L : tls) {
      ne += L.ep.size();
      nc += L.cp.size();
      nl += L.links.size();
    }
    fprintf(stderr, "ids=%llu cap=%llu links=%zu event_copies=%zu clicks=%zu\n",
            (unsigned long long)U.count.load(), (unsigned long long)U.cap, nl, ne, nc);
  }
  lg("events+clicks parsed");
  const int NC = (int)camp.keys.size();  // campaign index NC = organic
  const int NCAT = (int)cats.size();

  // ---- dedup per event-id partition: keep the copy with the smallest ingest_ts
  par(NP, [&](size_t p, int t) {
    size_t n = 0;
    for (auto& L : tls) n += L.ep.cnt[p] + L.ep.ov[p].size();
    size_t cap2 = 16;
    while (cap2 < n * 2) cap2 <<= 1;
    TL& L = tls[t];
    vector<ERec>& tab = L.dtab;  // whole records, so the final scan is sequential; h2 != 0 marks used
    tab.assign(cap2, ERec{0, 0, 0, 0, 0, 0});
    u64 mk = cap2 - 1;
    for (auto& X : tls)
      X.ep.each((int)p, [&](const ERec& r) {
        for (u64 i = r.h1 & mk;; i = (i + 1) & mk) {
          ERec& o = tab[i];
          if (!o.h2) {
            o = r;
            break;
          }
          if (o.h1 == r.h1 && o.h2 == r.h2) {
            if (r.ingest < o.ingest) o = r;
            break;
          }
        }
      });
    for (const ERec& r : tab)
      if (r.pc >> 31) L.pp.push(qpart(r.person), {(u64)r.person << 32 | r.ts, r.pc & 0xFFFFFF, (r.pc >> 24) & 127});
  });
  lg("dedup");

  // ---- sessionize + attribute per person partition
  for (auto& L : tls) {
    L.c_sess.assign((size_t)(NC + 1) * ND, 0);
    L.c_purch.assign((size_t)(NC + 1) * ND, 0);
    L.c_rev.assign((size_t)(NC + 1) * ND, 0);
    L.c_assist.assign((size_t)(NC + 1) * ND, 0);
  }
  vector<int> qorder(NQ);  // biggest partitions first, for balance
  {
    vector<size_t> qs(NQ, 0);
    for (auto& X : tls)
      for (int q = 0; q < NQ; q++) qs[q] += X.pp.cnt[q] + X.pp.ov[q].size();
    for (int q = 0; q < NQ; q++) qorder[q] = q;
    sort(qorder.begin(), qorder.end(), [&](int a, int b) { return qs[a] > qs[b]; });
  }
  par(NQ, [&](size_t qi, int t) {
    size_t q = (size_t)qorder[qi];
    TL& L = tls[t];
    // group by person (counting sort on a partition-local id), then sort each person's events
    // as packed u64 (ts << 31 | price << 7 | cat) and clicks as (ts << 32 | index into cka)
    double tA = now();
    size_t ne = 0;
    for (auto& X : tls) ne += X.pp.cnt[q] + X.pp.ov[q].size();
    size_t tsz = 16;
    while (tsz <= ne) tsz <<= 1;
    vector<u64>& tab = L.ltab;
    tab.assign(tsz, 0);
    auto slot_of = [&](u32 person) {
      u32 h = person;  // fmix32: independent of qpart's multiplicative hash
      h ^= h >> 16;
      h *= 0x85ebca6bu;
      h ^= h >> 13;
      h *= 0xc2b2ae35u;
      h ^= h >> 16;
      u64 i = h & (tsz - 1);
      while (tab[i] && (u32)(tab[i] >> 32) != person + 1) i = (i + 1) & (tsz - 1);
      return i;
    };
    vector<u32>& pers = L.pers;
    vector<u32>& lid = L.lid;
    vector<u32>& es = L.est_;
    pers.clear();
    lid.clear();
    es.clear();
    for (auto& X : tls)
      X.pp.each((int)q, [&](const PRec& r) {
        u32 person = (u32)(r.key >> 32);
        u64 i = slot_of(person);
        if (!tab[i]) {
          tab[i] = (u64)(person + 1) << 32 | pers.size();
          pers.push_back(person);
          es.push_back(0);
        }
        u32 g = (u32)tab[i];
        lid.push_back(g);
        es[g]++;
      });
    const u32 np = (u32)pers.size();
    es.push_back(0);
    for (u32 g = 0, run = 0; g <= np; g++) {
      u32 c = es[g];
      es[g] = run;
      run += c;
    }
    vector<u64>& ev = L.ev64;
    ev.resize(ne);
    vector<u32>& ep = L.epos;
    ep.assign(es.begin(), es.end());
    {
      size_t k = 0;
      for (auto& X : tls)
        X.pp.each((int)q, [&](const PRec& r) {
          ev[ep[lid[k++]]++] = (r.key & 0xFFFFFFFFull) << 31 | (u64)r.price << 7 | r.cat;
        });
    }
    vector<CRec>& cka = L.ckbuf;  // clicks of people with events
    vector<u32>& cs = L.cst_;
    cka.clear();
    lid.clear();
    cs.assign(np + 1, 0);
    for (auto& X : tls)
      X.cp.each((int)q, [&](const CRec& r) {
        u64 i = slot_of((u32)(r.key >> 32));
        if (!tab[i]) return;
        cka.push_back(r);
        lid.push_back((u32)tab[i]);
        cs[(u32)tab[i]]++;
      });
    for (u32 g = 0, run = 0; g <= np; g++) {
      u32 c = cs[g];
      cs[g] = run;
      run += c;
    }
    vector<u64>& ck = L.ck64;
    ck.resize(cka.size());
    vector<u32>& cpo = L.cpos;
    cpo.assign(cs.begin(), cs.end());
    for (size_t k = 0; k < cka.size(); k++) ck[cpo[lid[k]]++] = (u64)(u32)cka[k].key << 32 | k;
    double tB = now();
    for (u32 g = 0; g < np; g++) {
      sort_u64(ev.data() + es[g], es[g + 1] - es[g], 31, L.rtmp);
      sort_u64(ck.data() + cs[g], cs[g + 1] - cs[g], 32, L.rtmp);
    }
    double tC = now();
    vector<int> cnt(NC, 0), pos(NC, 0), active;
    vector<i64> catsum(NCAT, 0);
    auto cid = [&](u64 c) { const CRec& r = cka[(u32)c]; return string_view(r.cid, r.clen); };
    auto camp_of = [&](u64 c) { return (int)cka[(u32)c].camp; };
    for (u32 g = 0; g < np; g++) {
      size_t i = es[g], j = es[g + 1], ci = cs[g], cj = cs[g + 1];
      u32 person = pers[g];
      size_t lo = ci, hi = ci;
      i64 curday = INT64_MIN, d_sess = 0, d_ev = 0, d_rev = 0;
      auto flush = [&] {
        if (curday == INT64_MIN) return;
        int top = -1;
        if (d_rev > 0) {
          i64 bestv = 0;
          for (int c = 0; c < NCAT; c++)
            if (catsum[c] > bestv) {
              bestv = catsum[c];
              top = c;
            }
        }
        L.o_person.push_back(person);
        L.o_day.push_back((int32_t)curday);
        L.o_sess.push_back(d_sess);
        L.o_ev.push_back(d_ev);
        L.o_rev.push_back(d_rev);
        L.o_top.push_back(top);
        fill(catsum.begin(), catsum.end(), 0);
        d_sess = d_ev = d_rev = 0;
      };
      for (size_t k = i; k < j;) {
        u32 S = (u32)(ev[k] >> 31), prev = S;
        i64 day = floordiv(WS + S, DAY);
        if (day != curday) {
          flush();
          curday = day;
        }
        i64 rev = 0;
        size_t m = k;
        for (; m < j; m++) {
          u32 ts = (u32)(ev[m] >> 31);
          if (ts - prev > (u32)GAP) break;
          prev = ts;
          u32 price = (u32)(ev[m] >> 7) & 0xFFFFFF;
          rev += price;
          catsum[ev[m] & 127] += price;
        }
        d_sess++;
        d_ev += (i64)(m - k);
        d_rev += rev;
        // clicks in [S, S + WEEK] (click time base is WS - WEEK)
        u64 lim = (u64)S + WEEK;
        while (hi < cj && (ck[hi] >> 32) <= lim) {
          int c = camp_of(ck[hi]);
          if (cnt[c]++ == 0) {
            pos[c] = (int)active.size();
            active.push_back(c);
          }
          hi++;
        }
        while (lo < hi && (ck[lo] >> 32) < S) {
          int c = camp_of(ck[lo]);
          if (--cnt[c] == 0) {
            int last = active.back();
            active[pos[c]] = last;
            pos[last] = pos[c];
            active.pop_back();
          }
          lo++;
        }
        int di = (int)(day - WSDAY);
        int cr = NC;
        if (hi > lo) {  // latest click; among equal ts the smallest click_id
          size_t b = hi - 1, best = b;
          while (b > lo && (ck[b - 1] >> 32) == (ck[hi - 1] >> 32))
            if (cid(ck[--b]) < cid(ck[best])) best = b;
          cr = camp_of(ck[best]);
          for (int c : active) L.c_assist[(size_t)c * ND + di]++;
        }
        size_t ix = (size_t)cr * ND + di;
        L.c_sess[ix]++;
        if (rev > 0) {
          L.c_purch[ix]++;
          L.c_rev[ix] += rev;
        }
        k = m;
      }
      flush();
      for (int c : active) cnt[c] = 0;
      active.clear();
    }
    double tD = now();
    L.tm[0] += tB - tA;
    L.tm[1] += tC - tB;
    L.tm[2] += tD - tC;
  });
  {
    double a = 0, b = 0, c = 0;
    for (auto& L : tls) a += L.tm[0], b += L.tm[1], c += L.tm[2];
    fprintf(stderr, "sessions thread-sec: group %.3f sort %.3f walk %.3f\n", a, b, c);
  }
  lg("sessions");

  // ---- write outputs
  string ud = OUT + "/user_daily", cd = OUT + "/campaign_daily";
  mkdir(OUT.c_str(), 0777);
  mkdir(ud.c_str(), 0777);
  mkdir(cd.c_str(), 0777);
  par(T + 1, [&](size_t w, int) {
    if (w == (size_t)T) {
      vector<PCol> cols = {{"campaign_id", PQ_BYTES, false, 0, {}, {}},
                           {"channel", PQ_BYTES, false, 0, {}, {}},
                           {"day", PQ_INT32, false, 6, {}, {}},
                           {"sessions", PQ_INT64, false, -1, {}, {}},
                           {"purchasing_sessions", PQ_INT64, false, -1, {}, {}},
                           {"revenue_cents", PQ_INT64, false, -1, {}, {}},
                           {"assisted_sessions", PQ_INT64, false, -1, {}, {}}};
      size_t rows = 0;
      for (int c = 0; c <= NC; c++)
        for (int d = 0; d < ND; d++) {
          size_t ix = (size_t)c * ND + d;
          i64 s = 0, pu = 0, rv = 0, as = 0;
          for (auto& L : tls) {
            s += L.c_sess[ix];
            pu += L.c_purch[ix];
            rv += L.c_rev[ix];
            as += L.c_assist[ix];
          }
          if (!s && !as) continue;
          put_str(cols[0], c == NC ? string_view("organic") : camp.keys[c].sv());
          put_str(cols[1], c == NC ? string_view("organic") : chans[camp.val[c]].sv());
          put_i32(cols[2], (int32_t)(WSDAY + d));
          put_i64(cols[3], s);
          put_i64(cols[4], pu);
          put_i64(cols[5], rv);
          put_i64(cols[6], as);
          rows++;
        }
      write_parquet(cd + "/part-0.parquet", cols, rows);
      return;
    }
    TL& L = tls[w];
    if (L.o_person.empty() && w != 0) return;
    vector<PCol> cols = {{"user_id", PQ_BYTES, false, 0, {}, {}},
                         {"day", PQ_INT32, false, 6, {}, {}},
                         {"sessions", PQ_INT32, false, -1, {}, {}},
                         {"events", PQ_INT32, false, -1, {}, {}},
                         {"revenue_cents", PQ_INT64, false, -1, {}, {}},
                         {"top_category", PQ_BYTES, true, 0, {}, {}}};
    size_t rows = L.o_person.size();
    cols[0].data.s.reserve(rows * 24);
    for (int k = 1; k <= 4; k++) cols[k].data.s.reserve(rows * 8);
    cols[5].data.s.reserve(rows * 8);
    cols[5].def.reserve(rows);
    const vector<u32>& op = L.o_person;
    string_view name;
    for (size_t r = 0; r < rows; r++) {
      if (r + 16 < rows) __builtin_prefetch(&U.s[op[r + 16]]);  // names are random reads
      if (r + 8 < rows) __builtin_prefetch(U.s[op[r + 8]].ptr);
      if (r == 0 || op[r] != op[r - 1]) name = U.str(op[r]);
      put_str(cols[0], name);
      put_i32(cols[1], L.o_day[r]);
      put_i32(cols[2], (int32_t)L.o_sess[r]);
      put_i32(cols[3], (int32_t)L.o_ev[r]);
      put_i64(cols[4], L.o_rev[r]);
      int tc = L.o_top[r];
      cols[5].def.push_back(tc >= 0);
      if (tc >= 0) put_str(cols[5], cats[tc].sv());
    }
    write_parquet(ud + "/part-" + to_string(w) + ".parquet", cols, rows);
  });
  lg("written");
  {  // exit would tear these mappings down serially; doing it in parallel is faster
    vector<pair<void*, size_t>> big;
    for (auto& c : lch) big.push_back({(void*)c.b, (size_t)(c.e - c.b)});
    for (auto& c : ech) big.push_back({(void*)c.b, (size_t)(c.e - c.b)});
    for (auto& L : tls) {
      big.push_back({L.ep.base, L.ep.bytes});
      big.push_back({L.cp.base, L.cp.bytes});
      big.push_back({L.pp.base, L.pp.bytes});
    }
    big.push_back({U.s, U.cap * sizeof(Slot)});
    par(big.size(), [&](size_t i, int) {
      const size_t pg = 1 << 14;
      uintptr_t a = ((uintptr_t)big[i].first + pg - 1) & ~(uintptr_t)(pg - 1);
      uintptr_t b = ((uintptr_t)big[i].first + big[i].second) & ~(uintptr_t)(pg - 1);
      if (b > a) madvise((void*)a, b - a, MADV_DONTNEED);
    });
    lg("released");
  }
  fflush(stderr);
  _exit(0);
}
