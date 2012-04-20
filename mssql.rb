require 'benchmark'
require 'tiny_tds'
require './ctt'

puts "MSSQL benchmark"
puts "Building data cache..."

postal_codes = CTT::load_postal_codes

def f val
  return "null" if val == ""
  return "'" + val.gsub(/'/, "''") + "'" if val.class == String
  return val
end

def create_client  
  return TinyTds::Client.new :username => 'sa', :password => 'secret', :host => '10.224.69.62'
end

client = create_client

result = client.execute "use nosql_perf"
result.do

insert_benchmark = -> do  
  thread_count = 64
  threads = []
  
  page_size = postal_codes.length / thread_count
  
  (0..thread_count).each do |i|
    threads[i] = Thread.new do
      tclient = create_client
      
      result = tclient.execute "use nosql_perf"
      result.do
      
      lower_limit = i * page_size
      upper_limit = i * page_size + page_size
      upper_limit = postal_codes.length - 1 if upper_limit >= postal_codes.length - 1
      
      postal_codes[lower_limit..upper_limit].each do |c|
        stmt = "insert into postal_codes (district_id, county_id, place_id, place_name,
            artery, artery_type, first_preposition, artery_title, second_prepositon,
            artery_name, artery_local, sketch, door, client_name, postal_code,
            postal_code_ext, postal_code_name) 
          values (#{f c.district_id}, #{f c.county_id}, #{f c.place_id}, #{f c.place_name}, 
            #{f c.artery}, #{f c.artery_type}, #{f c.first_preposition}, #{f c.artery_title}, 
            #{f c.second_prepositon}, #{f c.artery_name}, #{f c.artery_local}, #{f c.sketch}, 
            #{f c.door}, #{f c.client_name}, #{f c.postal_code}, #{f c.postal_code_ext}, 
            #{f c.postal_code_name})"
    
        result = tclient.execute stmt
        result.insert
      end 
    end
  end
  
  threads.each { |thread| thread.join }
end

query_benchmark = -> do
  thread_count = 64
  threads = []
  
  page_size = postal_codes.length / thread_count
  
  (0..thread_count).each do |i|
    threads[i] = Thread.new do
      tclient = create_client
      
      result = tclient.execute "use nosql_perf"
      result.do
      
      lower_limit = i * page_size
      upper_limit = i * page_size + page_size
      upper_limit = postal_codes.length - 1 if upper_limit >= postal_codes.length - 1
      
      (lower_limit..upper_limit).each do |key|
         stmt = "select district_id, county_id, place_id, place_name,
            artery, artery_type, first_preposition, artery_title, second_prepositon,
            artery_name, artery_local, sketch, door, client_name, postal_code,
            postal_code_ext, postal_code_name
          from postal_codes
          where _id=#{key}"
    
        result = tclient.execute stmt
        rows = result.each
      end 
    end
  end
  
  threads.each { |thread| thread.join }
end

puts "Benchmarking"

Benchmark.bm 7 do |b|
  (1...10).each do |run|
    result = client.execute "truncate table postal_codes"
    result.do
    b.report("i: #{run}") { insert_benchmark.call }
    b.report("q: #{run}") { query_benchmark.call }
  end 
end
