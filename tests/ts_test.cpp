// Self-check: the fast timestamp parser agrees with the plain one on edge cases and fuzz.
// c++ -std=c++17 -O1 -Dmain=pipeline_main -o ts_test tests/ts_test.cpp && ./ts_test
#include "../src/main.cpp"
#undef main
#include <random>

static int fails = 0;
static void check(const string& v) {
  string buf(16, 'x');  // >= 16 readable bytes before and after, like a data line
  buf += v;
  buf += string(16, ',');
  i64 a = 0, b = 0;
  bool fa = parse_ts(buf.data() + 16, (u32)v.size(), a), fb = parse_ts_slow(buf.data() + 16, (u32)v.size(), b);
  if (fa != fb || (fa && a != b)) {
    fails++;
    printf("MISMATCH %s: fast %d %lld slow %d %lld\n", v.c_str(), fa, (long long)a, fb, (long long)b);
  }
}

int main() {
  init_ym();
  i64 v;
  string ex = "xxxxxxxxxxxxxxxx2026-10-03T19:58:10-04:00,,,,,,,,,,,,,,,,";
  if (!parse_ts(ex.data() + 16, 25, v) || v != 1791071890000) fails++, puts("F2 example");
  for (const char* s :
       {"2026-10-03T23:58:10Z", "1791071890000", "0001791071890000", "86400000", "0", "1", "",
        "2028-02-29T00:00:00Z", "2027-02-29T00:00:00Z", "2026-02-30T00:00:00Z", "2026-13-01T00:00:00Z",
        "2026-10-03T24:00:00Z", "2026-10-03T23:60:00Z", "2026-10-03T23:00:60Z", "2026-10-03t23:58:10Z",
        "2026-10-03T23:58:10z", "2026-10-03 23:58:10Z", "2026-10-03T23:58:10", "2026-10-03T23:58:10.000Z",
        "2026-10-03T19:58:10+24:00", "2026-10-03T19:58:10+05:60", "2026-10-03T19:58:10+0200",
        "2026-10-03T19:58:10-00:00", "2026-10-03T19:58:10+23:59", "1791071890000.0", "1.79e12", "-5",
        "1800-01-01T00:00:00Z", "2400-01-01T00:00:00+01:00", "0000-01-01T00:00:00Z", "9999-12-31T23:59:59Z",
        "12345678901234567", "000000000000000000001", "99999999999999999999", "9223372036854775807",
        "9223372036854775808", "1234567890123456"})
    check(s);
  mt19937_64 rng(7);
  const char alpha[] = "0123456789-:TZ+ z.";
  for (int it = 0; it < 2000000; it++) {
    string s;
    if (it % 3 == 0) {  // mutate a valid F1/F2
      s = it % 2 ? "2027-03-14T15:09:26Z" : "2027-03-14T15:09:26+05:30";
      for (int k = rng() % 3; k-- > 0;) s[rng() % s.size()] = alpha[rng() % (sizeof alpha - 1)];
    } else {
      int n = rng() % 26;
      for (int k = 0; k < n; k++) s += (rng() % 8) ? char('0' + rng() % 10) : alpha[rng() % (sizeof alpha - 1)];
    }
    check(s);
  }
  printf("%s (%d mismatches)\n", fails ? "FAIL" : "ok", fails);
  return fails != 0;
}
