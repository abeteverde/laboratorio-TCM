const mongoose = require('mongoose');

const talk_schema = new mongoose.Schema({
    _id:String,
    title: String,
    url: String,
    watch_next_ids:[String]
}, { collection: 'tedz_data' });

module.exports = mongoose.model('talk', talk_schema);
