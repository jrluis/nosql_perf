require 'json'
require 'benchmark'
require 'cassandra-cql'
require './ctt'

puts "Cassandra benchmark"
puts "Building data cache..."

postal_codes = CTT::load_postal_codes
postal_codes.collect! { |postal_code| postal_code.to_hash.to_json }

def create_connection  
  return CassandraCQL::Database.new '10.58.169.226:9160', {:keyspace => 'nosql_perf'}, {:retries => 3, :timeout => 5, :connect_timeout => 20}
end

db = create_connection

insert_benchmark = -> do
  thread_count = 2
  threads = []
  
  page_size = postal_codes.length / thread_count
  
  (0..thread_count).each do |i|
    threads[i] = Thread.new do
      tdb = create_connection
      
      lower_limit = i * page_size
      upper_limit = i * page_size + page_size
      upper_limit = postal_codes.length - 1 if upper_limit >= postal_codes.length - 1
      
      key = i * page_size + 1
      postal_codes[lower_limit..upper_limit].each do |postal_code|
      	begin
        	#tdb.execute 'insert into postal_codes (key, value) values (?, ?)', key, postal_code
        	tdb.execute 'update postal_codes set value=? where key=?', postal_code, key
        rescue => e
        	puts e.to_s
        	raise
        end
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
      tdb = create_connection
      
      lower_limit = i * page_size
      upper_limit = i * page_size + page_size
      upper_limit = postal_codes.length - 1 if upper_limit >= postal_codes.length - 1
      
      (lower_limit..upper_limit).each do |key|
         result = tdb.execute 'select value from postal_codes where key=?', key
         postal_code = result.fetch_row['value']
      end 
    end
  end
  
  threads.each { |thread| thread.join }
end

puts "Benchmarking"

Benchmark.bm 7 do |b|
  (1...10).each do |run|
	#db.execute 'truncate postal_codes'
	b.report("i: #{run}") { insert_benchmark.call }
    b.report("q: #{run}") { query_benchmark.call } 
  end 
end
