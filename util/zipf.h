
#include <atomic>
/* extern long items; //initialized in init_zipf_generator function
extern long base; //initialized in init_zipf_generator function
extern double zipfianconstant; //initialized in init_zipf_generator function
extern double alpha; //initialized in init_zipf_generator function
extern double zetan; //initialized in init_zipf_generator function
extern double eta; //initialized in init_zipf_generator function
extern double theta; //initialized in init_zipf_generator function
extern double zeta2theta; //initialized in init_zipf_generator function
extern long countforzeta; //initialized in init_zipf_generator function
extern long lastVal; //initialized in setLastValue */

class ZipfGenerator {
 private:
  std::atomic<long> countnow;
  long items;              // initialized in init_zipf_generator function
  long base;               // initialized in init_zipf_generator function
  double zipfianconstant;  // initialized in init_zipf_generator function
  double alpha;            // initialized in init_zipf_generator function
  double zetan;            // initialized in init_zipf_generator function
  double eta;              // initialized in init_zipf_generator function
  double theta;            // initialized in init_zipf_generator function
  double zeta2theta;       // initialized in init_zipf_generator function
  long countforzeta;       // initialized in init_zipf_generator function
  long lastVal;            // initialized in setLastValue

 public:
  void init_zipf_generator(long min, long max);
  void init_latest_generator(long min, long max);
  double zeta(long st, long n, double initialsum);
  double zetastatic(long st, long n, double initialsum);
  long nextLong(long itemcount);
  long nextValue();
  long nextLatestValue();
  void setLastValue(long val);
};
