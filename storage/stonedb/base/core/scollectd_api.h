/*
 * Copyright 2015 Cloudius Systems
 * Copyright (c) 2022 StoneAtom, Inc. All rights reserved.
 */
#ifndef STONEDB_BASE_SCOLLECTD_API_H_
#define STONEDB_BASE_SCOLLECTD_API_H_
#pragma once

#include "base/core/metrics_api.h"
#include "base/core/scollectd.h"

namespace stonedb {
namespace base {

namespace scollectd {
using collectd_value = base::metrics::impl::metric_value;

std::vector<collectd_value> get_collectd_value(const scollectd::type_instance_id &id);

std::vector<scollectd::type_instance_id> get_collectd_ids();

sstring get_collectd_description_str(const scollectd::type_instance_id &);

bool is_enabled(const scollectd::type_instance_id &id);
/**
 * Enable or disable collectd metrics on local instance
 * @id - the metric to enable or disable
 * @enable - should the collectd metrics be enable or disable
 */
void enable(const scollectd::type_instance_id &id, bool enable);

metrics::impl::value_map get_value_map();
}  // namespace scollectd

}  // namespace base
}  // namespace stonedb

#endif  // STONEDB_BASE_SCOLLECTD_API_H_
