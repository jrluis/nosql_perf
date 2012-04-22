require 'benchmark'
require 'redis'
require './ctt'

puts "Redis benchmark"
puts "Building data cache..."

postal_codes = CTT::load_postal_codes
postal_codes.collect! { |postal_code| postal_code.to_hash }

def create_connection  
  return Redis.new :host => "10.0.1.1", :port => 6380
end

conn = create_connection
conn.select 'nosql_perf'

insert_benchmark = -> do 
  thread_count = 64
  threads = []
  
  page_size = postal_codes.length / thread_count
  
  (0..thread_count).each do |i|
    threads[i] = Thread.new do
      tconn = create_connection
      conn.select 'nosql_perf'
      
      lower_limit = i * page_size
      upper_limit = i * page_size + page_size
      upper_limit = postal_codes.length - 1 if upper_limit >= postal_codes.length - 1
      
      key = i * page_size + 1
      postal_codes[lower_limit..upper_limit].each do |postal_code|
        tconn.set key = postal_code
        key += 1
      end 
    end
  end
end

query_benchmark = -> do
  thread_count = 64
  threads = []
  
  page_size = postal_codes.length / thread_count
  
  (0..thread_count).each do |i|
    threads[i] = Thread.new do
      tconn = create_connection
      conn.select 'nosql_perf'
      
      lower_limit = i * page_size
      upper_limit = i * page_size + page_size
      upper_limit = postal_codes.length - 1 if upper_limit >= postal_codes.length - 1
      
      (lower_limit..upper_limit).each do |key|
        postal_code = tconn.get key
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
