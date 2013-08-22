'use strict';

var _ = require("underscore"),
    metalog    = require('../metalog'),
    mongo      = require('mongodb'),
    Model      = require("../core_ext/model"),
    hasher     = require('../expression-hasher');

var second   = 1e3,
    second10 = 10e3,
    minute   = 60e3,
    minute5  = 300e3,
    hour     = 3600e3,
    day      = 86400e3;

var tiers  = require("../tiers"),
    tensec = tiers[second10],
    type_re = /^[a-z][a-zA-Z0-9_]+$/,
    metric_fields = {v: 1, vs: 1},
    metric_options = {sort: {"_id.t": 1}, batchSize: 1000};

function Metric(data, measurement, values){
  this.time  = data.time;
  this.value = data.value;
  if('group' in data) this.group = data.group === undefined ? null : data.group;

  this.setProperty("values", { value: values||[] });
  this.setProperty("measurement", { value: measurement });
}

Model.modelize(Metric);

Metric.setProperties({
  tier:    { get: function(){ return this.measurement.tier } },
  bin:     { get: function(){ return this.tier.bin(this.time); } },
  e:       { get: function(){ return this.measurement.expression.source }},
  l:       { get: function(){ return this.measurement.tier.key }},
  type:    { get: function(){ return this.measurement.expression.type }},

  to_wire: { value: to_wire },
  report:  { value: report  },
  save:    { value: save }
});

function find(db, measurement, callback){
  var expression = measurement.expression,
      start = measurement.start,
      stop  = measurement.stop,
      type  = expression.type,
      tier  = measurement.tier;

  db.metrics(type, function(error, collection){
    if (error) return callback(error);

    var query = {
      i: false,
      "_id.e": hasher.hash(expression.source),
      "_id.l": tier.key,
      "_id.t": {
        $gte: start,
        $lt: stop
      }
    };
    if ('group' in measurement) query["_id.g"] = measurement.group;

    collection.find(query, metric_fields, metric_options, handleResponse);
  });

  function handleResponse(error, cursor){
    if (error) return callback(error);
    cursor.each(function(error, row) {
      if (error) return callback(error);
      if (row) callback(error, Metric.from_wire(row, measurement));
      else callback();
    })
  }
}
Object.defineProperty(Metric, "find", { value: find });
Object.defineProperty(Metric, "from_wire", { value: from_wire });

function from_wire(row, measurement){
  var values = null,
      data;
  if(!measurement.isPyramidal) {
    values = measurement.unwrap(row.vs);
  }

  data = {time: row._id.t, value: row.v};
  if('g' in row._id) data.group = row._id.g;

  return new Metric(data, measurement, values);
}

function to_wire(){
  var values = null,
      id;
  if(!this.measurement.isPyramidal){
    values = this.measurement.wrap(this.values);
  }
  id = { e: hasher.hash(this.e), l: this.l, t: this.time };
  if('group' in this) id.g = this.group;

  if (this.value === undefined) {
    return { i: false, vs: values, _id: id};
  } else {
    return { i: false, v: mongo.Double(this.value), vs: values, _id: id};
  }
};

function report(){
  var hsh = { time: this.time, value: this.value };
  return hsh;
};

function save(db, callback){
  var self = this;
  if (this.validate) this.validate();

  db.metrics(self.type, function(error, collection){
    if (error) return callback(error);
    collection.save(self.to_wire(), function(error){
      callback(error, self);
    });
  });
};

module.exports = Metric;
