#ifndef __BRUBECK_HISTO_H__
#define __BRUBECK_HISTO_H__

struct brubeck_histo {
  value_t *values;
  uint32_t count;
  uint16_t alloc, size;
};

#define METRIC_TYPES 13  // size of metric_id_t
#define METRIC_TYPES_PCNT 6
enum metric_id_t  { PC_75, PC_95, PC_98, PC_99, PC_999, PC_50, MIN, MAX, SUM, MEAN, MEDIAN, COUNT, RATE };

struct brubeck_histo_sample {
  value_t sum;
  value_t min;
  value_t max;
  value_t mean;
  value_t median;
  value_t count;

  value_t percentile[METRIC_TYPES_PCNT];
};


struct metric_options_t {
  char send[METRIC_TYPES_PCNT];
  int max_length;
};

void metric_options_default(struct metric_options_t *metric_options);

void brubeck_histo_push(struct brubeck_histo *histo, value_t value,
                        value_t sample_rate);
void brubeck_histo_sample(struct brubeck_histo_sample *sample,
                          struct brubeck_histo *histo,
                          struct metric_options_t *metric_options);

#endif
