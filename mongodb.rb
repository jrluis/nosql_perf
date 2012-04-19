require 'benchmark'
require 'mongo'
require './ctt'

puts "MongoDB benchmark"
puts "Building data cache..."

postal_codes = CTT::load_postal_codes
postal_codes.collect! { |postal_code| postal_code.to_hash }

host = 'ec2-176-34-193-128.eu-west-1.compute.amazonaws.com'
conn = Mongo::Connection.new host
db = conn.db "nosql_perf"

insert_benchmark = -> do 
  thread_count = 2
  threads = []
  
  page_size = postal_codes.length / thread_count
  
  (0..thread_count).each do |i|
    threads[i] = Thread.new do
      tconn = Mongo::Connection.new host
      tdb = conn.db "nosql_perf"
      
      lower_limit = i * page_size
      upper_limit = i * page_size + page_size
      upper_limit = postal_codes.length - 1 if upper_limit >= postal_codes.length - 1
      
      key = i * page_size + 1
      postal_codes[lower_limit..upper_limit].each do |postal_code|
        postal_code['_id'] = key
        tdb["postal_codes"].insert postal_code 
        key += 1
      end 
    end
  end
end

query_benchmark = -> do
  thread_count = 2
  threads = []
  
  page_size = postal_codes.length / thread_count
  
  (0..thread_count).each do |i|
    threads[i] = Thread.new do
      tconn = Mongo::Connection.new host
      tdb = conn.db "nosql_perf"
      
      lower_limit = i * page_size
      upper_limit = i * page_size + page_size
      upper_limit = postal_codes.length - 1 if upper_limit >= postal_codes.length - 1
      
      (lower_limit..upper_limit).each do |key|
        postal_code = tdb['postal_codes'].find_one :_id => key
      end 
    end
  end
  
  threads.each { |thread| thread.join }
end

puts "Benchmarking"

Benchmark.bm 7 do |b|
  (1...10).each do |run|
    db.drop_collection "postal_codes"
    b.report("i: #{run}") { insert_benchmark.call }
    b.report("q: #{run}") { query_benchmark.call }
  end 
end
