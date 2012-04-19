require 'benchmark'
require 'cassandra'
require './ctt'

puts "Cassandra benchmark"
puts "Building data cache..."

postal_codes = CTT::load_postal_codes
postal_codes.collect! { |postal_code| postal_code.to_hash.to_json.force_encoding("ASCII-8BIT") }

client = Cassandra.new "nosql_perf", "cassandra:9160", :retries => 3, :timeout => 5, :connect_timeout => 1

insert_benchmark = -> do
  thread_count = 2
  threads = []
  
  page_size = postal_codes.length / thread_count
  
  (0..thread_count).each do |i|
    threads[i] = Thread.new do
      tclient = Cassandra.new "nosql_perf", "cassandra:9160", :retries => 3, :timeout => 5, :connect_timeout => 15
      
      lower_limit = i * page_size
      upper_limit = i * page_size + page_size
      upper_limit = postal_codes.length - 1 if upper_limit >= postal_codes.length - 1
      
      key = i * page_size + 1
      postal_codes[lower_limit..upper_limit].each do |postal_code|
        tclient.insert :postal_codes, key.to_s, {"postal_code" => postal_code}
        key += 1
      end 
    end
  end
  
  threads.each { |thread| thread.join }
end

query_benchmark = -> do
  thread_count = 2
  threads = []
  
  page_size = postal_codes.length / thread_count
  
  (0..thread_count).each do |i|
    threads[i] = Thread.new do
      tclient = Cassandra.new "nosql_perf", "cassandra:9160", :retries => 3, :timeout => 5, :connect_timeout => 5
      
      lower_limit = i * page_size
      upper_limit = i * page_size + page_size
      upper_limit = postal_codes.length - 1 if upper_limit >= postal_codes.length - 1
      
      (lower_limit..upper_limit).each do |key|
         postal_code = tclient.get :postal_codes, key.to_s, "postal_code"
      end 
    end
  end
  
  threads.each { |thread| thread.join }
end

puts "Benchmarking"

Benchmark.bm 7 do |b|
  (1...10).each do |run|
    client.clear_column_family! :postal_codes
    b.report("i: #{run}") { insert_benchmark.call }
    b.report("q: #{run}") { query_benchmark.call } 
  end 
end
