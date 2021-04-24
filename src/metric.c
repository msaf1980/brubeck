#include "brubeck.h"

/* enum metric_id_t  { PC_75, PC_95, PC_98, PC_99, PC_999, PC_50, MIN, MAX, SUM, MEAN, MEDIAN, COUNT, COUNT_PS, RATE }; */
static char *metric_names_t[] = {
  ".percentile.75", ".percentile.95", ".percentile.98", ".percentile.99", ".percentile.999", ".percentile.50", 
  ".min", ".max", ".sum", ".mean", ".median", ".count", ".count_ps"
};


static struct metric_options_t metric_options;


struct metric_names_s {
  char *p75;
  char *p95;
  char *p98;
  char *p99;
  char *p999;
  char *p50;

  char *min;
  char *max;
  char *sum;
  char *mean;
  char *median;
  char *count;
  char *rate;
};

int metric_name_change(const char *descr, enum metric_id_t  t, char *new_name, char *metric_names[]) {
  if (new_name == NULL) {
    return 0;
  }
  if (strlen(new_name) <= 1) {
    log_splunk("metric_name[%s]=invalid_string", descr);
    return -1;
  }
  if (new_name[0] != '.') {
   log_splunk("metric_name='%s' metric_names[%s]=name_without_dot", new_name, descr);
    return -1;
  }
  if (strcmp(new_name, metric_names[t]) != 0) {
    metric_names[t] = new_name;
  }
  return 0;
}

int load_metric_options(json_t *percentiles, json_t *metric_names) {
  size_t idx;
  json_t *s;
  int i;

  if (json_array_size(percentiles) == 0)  {
    metric_options_default(&metric_options);
  } else {
    for (i = 0; i < METRIC_TYPES_PCNT; i++) {
      metric_options.send[i] = 0;
    }
    i = 0;
    json_array_foreach(percentiles, idx, s) {
      if (s && json_typeof(s) == JSON_INTEGER) {
        int pcnt = json_integer_value(s);
        switch (pcnt) {
          case 50:
            metric_options.send[PC_50] = 1;
            break;
          case 75:
            metric_options.send[PC_75] = 1;
            break;
          case 95:
            metric_options.send[PC_95] = 1;
            break;
          case 98:
            metric_options.send[PC_98] = 1;
            break;
          case 99:
            metric_options.send[PC_99] = 1;
            break;
          case 999:
            metric_options.send[PC_999] = 1;
            break;
          default:
            log_splunk("percentile=%d percentiles[%d]=invalid_percentile", pcnt, i);
            return -1;
        }
      } else {
        log_splunk("percentile=error percentiles[%d]=invalid_int", i);
        return -1;
      }
      i++;
    }
  }

  if (metric_names && json_typeof(metric_names) == JSON_OBJECT) {
    struct metric_names_s m;
    m.p75 = NULL;
    m.p95 = NULL;
    m.p98 = NULL;
    m.p99 = NULL;
    m.p999 = NULL;
    m.p50 = NULL;
    m.min = NULL;
    m.max = NULL;
    m.sum = NULL;
    m.mean = NULL;
    m.median = NULL;
    m.count = NULL;
    m.rate = NULL;

    json_unpack_or_die(metric_names, "{s?:s, s?:s, s?:s, s?:s, s?:s, s?:s, s?:s, s?:s, s?:s, s?:s, s?:s, s?:s, s?:s}",
                  "p75", &m.p75,
                  "p95", &m.p95,
                  "p98", &m.p98,
                  "p99", &m.p99,
                  "p999", &m.p999,
                  "p50", &m.p50,
                  "min", &m.min,
                  "max", &m.max,
                  "sum", &m.sum,
                  "mean", &m.mean,
                  "median", &m.median,
                  "count", &m.count,
                  "rate", &m.rate
    );
    if (metric_name_change("p75", PC_75, m.p75, metric_names_t)) {
      return -1;
    }
    if (metric_name_change("p95", PC_95, m.p95, metric_names_t)) {
      return -1;
    }
    if (metric_name_change("p98", PC_98, m.p98, metric_names_t)) {
      return -1;
    }
    if (metric_name_change("p99", PC_99, m.p99, metric_names_t)) {
      return -1;
    }
    if (metric_name_change("p999", PC_999, m.p999, metric_names_t)) {
      return -1;
    }
    if (metric_name_change("min", MIN, m.min, metric_names_t)) {
      return -1;
    }
    if (metric_name_change("max", MAX, m.max, metric_names_t)) {
      return -1;
    }
    if (metric_name_change("sum", SUM, m.sum, metric_names_t)) {
      return -1;
    }
    if (metric_name_change("count", COUNT, m.count, metric_names_t)) {
      return -1;
    }
    if (metric_name_change("rate", RATE, m.rate, metric_names_t)) {
      return -1;
    }
  } else {
    log_splunk("metric_names=error metric_names=invalid_obect");
    return -1;
  }

  metric_options.max_length = 0;
  for (i = 0; i < METRIC_TYPES; i++) {
    size_t len = strlen(metric_names_t[i]);
    if (metric_options.max_length < len) {
      metric_options.max_length = len;
    }
  }

  return 0;
}

static inline struct brubeck_metric *new_metric(struct brubeck_server *server,
                                                const char *key, size_t key_len,
                                                uint8_t type) {
  struct brubeck_metric *metric;
  const struct brubeck_tag_set *tags = NULL;

  if (server->tags) {
    tags = brubeck_get_tag_set(server->tags, key, key_len);
    if (tags)
      // we have parsed the tags, remove them from the metric
      key_len = key_len - tags->tag_len;
  }

  /* slab allocation cannot fail */
  metric = brubeck_slab_alloc(&server->slab,
                              sizeof(struct brubeck_metric) + key_len + 1);

  memset(metric, 0x0, sizeof(struct brubeck_metric));

  metric->tags = tags;
  memcpy(metric->key, key, key_len);
  metric->key[key_len] = '\0';
  metric->key_len = (uint16_t)key_len;

  brubeck_metric_set_state(metric, BRUBECK_STATE_ACTIVE);
  metric->type = type;
  pthread_spin_init(&metric->lock, PTHREAD_PROCESS_PRIVATE);

#ifdef BRUBECK_METRICS_FLOW
  metric->flow = 0;
#endif

  return metric;
}

typedef void (*mt_prototype_record)(struct brubeck_metric *, value_t, value_t,
                                    uint8_t);
typedef void (*mt_prototype_sample)(struct brubeck_metric *, brubeck_sample_cb,
                                    void *);

/*********************************************
 * Gauge
 *
 * ALLOC: mt + 4 bytes
 *********************************************/
static void gauge__record(struct brubeck_metric *metric, value_t value,
                          value_t sample_freq, uint8_t modifiers) {
  pthread_spin_lock(&metric->lock);
  {
    if (modifiers & BRUBECK_MOD_RELATIVE_VALUE) {
      metric->as.gauge.value += value;
    } else {
      metric->as.gauge.value = value;
    }
  }
  pthread_spin_unlock(&metric->lock);
}

static void gauge__sample(struct brubeck_metric *metric,
                          brubeck_sample_cb sample, void *opaque) {
  value_t value;

  pthread_spin_lock(&metric->lock);
  { value = metric->as.gauge.value; }
  pthread_spin_unlock(&metric->lock);

  sample(metric, metric->key, value, opaque);
}

/*********************************************
 * Meter
 *
 * ALLOC: mt + 4
 *********************************************/
static void meter__record(struct brubeck_metric *metric, value_t value,
                          value_t sample_freq, uint8_t modifiers) {
  /* upsample */
  value *= sample_freq;

  pthread_spin_lock(&metric->lock);
  { metric->as.meter.value += value; }
  pthread_spin_unlock(&metric->lock);
}

static void meter__sample(struct brubeck_metric *metric,
                          brubeck_sample_cb sample, void *opaque) {
  value_t value;

  pthread_spin_lock(&metric->lock);
  {
    value = metric->as.meter.value;
    metric->as.meter.value = 0.0;
  }
  pthread_spin_unlock(&metric->lock);

  sample(metric, metric->key, value, opaque);
}

/*********************************************
 * Counter
 *
 * ALLOC: mt + 4 + 4 + 4
 *********************************************/
static void counter__record(struct brubeck_metric *metric, value_t value,
                            value_t sample_freq, uint8_t modifiers) {
  /* upsample */
  value *= sample_freq;

  pthread_spin_lock(&metric->lock);
  {
    if (metric->as.counter.previous > 0.0) {
      value_t diff = (value >= metric->as.counter.previous)
                         ? (value - metric->as.counter.previous)
                         : (value);

      metric->as.counter.value += diff;
    }

    metric->as.counter.previous = value;
  }
  pthread_spin_unlock(&metric->lock);
}

static void counter__sample(struct brubeck_metric *metric,
                            brubeck_sample_cb sample, void *opaque) {
  value_t value;

  pthread_spin_lock(&metric->lock);
  {
    value = metric->as.counter.value;
    metric->as.counter.value = 0.0;
  }
  pthread_spin_unlock(&metric->lock);

  sample(metric, metric->key, value, opaque);
}

/*********************************************
 * Histogram / Timer
 *
 * ALLOC: mt + 16 + 4
 *********************************************/
static void histogram__record(struct brubeck_metric *metric, value_t value,
                              value_t sample_freq, uint8_t modifiers) {
  pthread_spin_lock(&metric->lock);
  { brubeck_histo_push(&metric->as.histogram, value, sample_freq); }
  pthread_spin_unlock(&metric->lock);
}

static void histogram__sample(struct brubeck_metric *metric,
                              brubeck_sample_cb sample, void *opaque) {
  struct brubeck_histo_sample hsample;
  char *key;

  pthread_spin_lock(&metric->lock);
  { brubeck_histo_sample(&hsample, &metric->as.histogram, &metric_options); }
  pthread_spin_unlock(&metric->lock);

  /* alloc space for this on the stack. we need enough for:
   * key_length + longest_suffix + null terminator
   */
  key = alloca(metric->key_len + metric_options.max_length + 1);
  memcpy(key, metric->key, metric->key_len);

  WITH_SUFFIX(metric_names_t[COUNT]) { sample(metric, key, hsample.count, opaque); }

  WITH_SUFFIX(metric_names_t[RATE]) {
    struct brubeck_backend *backend = opaque;
    sample(metric, key, hsample.count / (double)backend->sample_freq, opaque);
  }

  /* if there have been no metrics during this sampling period,
   * we don't need to report any of the histogram samples */
  if (hsample.count == 0.0)
    return;

  WITH_SUFFIX(metric_names_t[MIN]) { sample(metric, key, hsample.min, opaque); }

  WITH_SUFFIX(metric_names_t[MAX]) { sample(metric, key, hsample.max, opaque); }

  WITH_SUFFIX(metric_names_t[SUM]) { sample(metric, key, hsample.sum, opaque); }

  WITH_SUFFIX(metric_names_t[MEAN]) { sample(metric, key, hsample.mean, opaque); }

  WITH_SUFFIX(metric_names_t[MEDIAN]) { sample(metric, key, hsample.median, opaque); }

  if (metric_options.send[PC_50]) {
    WITH_SUFFIX(metric_names_t[PC_50]) { sample(metric, key, hsample.percentile[PC_50], opaque); }
  }

  if (metric_options.send[PC_75]) {
    WITH_SUFFIX(metric_names_t[PC_75]) { sample(metric, key, hsample.percentile[PC_75], opaque); }
  }

  if (metric_options.send[PC_95]) {
    WITH_SUFFIX(metric_names_t[PC_95]) { sample(metric, key, hsample.percentile[PC_95], opaque); }
  }

  if (metric_options.send[PC_98]) {
    WITH_SUFFIX(metric_names_t[PC_98]) { sample(metric, key, hsample.percentile[PC_98], opaque); }
  }

if (metric_options.send[PC_99]) {
    WITH_SUFFIX(metric_names_t[PC_99]) { sample(metric, key, hsample.percentile[PC_99], opaque); }
  }

  if (metric_options.send[PC_999]) {
    WITH_SUFFIX(metric_names_t[PC_999]) { sample(metric, key, hsample.percentile[PC_999], opaque); }
  }
}

/********************************************************/

static struct brubeck_metric__proto {
  mt_prototype_record record;
  mt_prototype_sample sample;
} _prototypes[] = {
    /* Gauge */
    {&gauge__record, &gauge__sample},

    /* Meter */
    {&meter__record, &meter__sample},

    /* Counter */
    {&counter__record, &counter__sample},

    /* Histogram */
    {&histogram__record, &histogram__sample},

    /* Timer -- uses same implementation as histogram */
    {&histogram__record, &histogram__sample},

    /* Internal -- used for sampling brubeck itself */
    {NULL, /* recorded manually */
     brubeck_internal__sample}};

void brubeck_metric_sample(struct brubeck_metric *metric, brubeck_sample_cb cb,
                           void *backend) {
  _prototypes[metric->type].sample(metric, cb, backend);
}

void brubeck_metric_record(struct brubeck_metric *metric, value_t value,
                           value_t sample_freq, uint8_t modifiers) {
  brubeck_metric_set_state(metric, BRUBECK_STATE_ACTIVE);
  _prototypes[metric->type].record(metric, value, sample_freq, modifiers);
}

struct brubeck_backend *brubeck_metric_shard(struct brubeck_server *server,
                                             struct brubeck_metric *metric) {
  int shard = 0;
  if (server->active_backends > 1)
    shard = CityHash32(metric->key, metric->key_len) % server->active_backends;
  return server->backends[shard];
}

struct brubeck_metric *brubeck_metric_new(struct brubeck_server *server,
                                          const char *key, size_t key_len,
                                          uint8_t type) {
  struct brubeck_metric *metric;
  // key is part of a shared buffer that will change, so a copy is required
  char *key_for_ht = strndup(key, key_len);

  metric = new_metric(server, key, key_len, type);
  if (!metric)
    return NULL;

  if (!brubeck_hashtable_insert(server->metrics, key_for_ht, key_len, metric)) {
    free(key_for_ht);
    return brubeck_hashtable_find(server->metrics, key, key_len);
  }
  brubeck_backend_register_metric(brubeck_metric_shard(server, metric), metric);

  /* Record internal stats */
  brubeck_stats_inc(server, unique_keys);
  return metric;
}

struct brubeck_metric *brubeck_metric_find(struct brubeck_server *server,
                                           const char *key, size_t key_len,
                                           uint8_t type) {
  struct brubeck_metric *metric;

  assert(key[key_len] == '\0');
  metric = brubeck_hashtable_find(server->metrics, key, (uint16_t)key_len);

  if (unlikely(metric == NULL)) {
    if (server->at_capacity)
      return NULL;

    return brubeck_metric_new(server, key, key_len, type);
  }

#ifdef BRUBECK_METRICS_FLOW
  brubeck_atomic_inc(&metric->flow);
#endif

  return metric;
}
