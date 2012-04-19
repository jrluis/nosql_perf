class Struct
  def to_hash
    Hash[*members.zip(values).flatten]
  end
end

class Hash
  def to_struct name 
    Struct.new(name, *keys).new *values
  end
end

module CTT
  
  District = Struct.new :id, :name
  County = Struct.new :district_id, :id, :name
  
  PostalCode = Struct.new :district_id, :county_id, :place_id, :place_name,
    :artery, :artery_type, :first_preposition, :artery_title, :second_prepositon,
    :artery_name, :artery_local, :sketch, :door, :client_name, :postal_code,
    :postal_code_ext, :postal_code_name
  
  def self.load_districts
    file = File.open "data/distritos.txt", "r:iso-8859-1:utf-8"
    
    districts = []
    
    file.each_line do |line|
      fields = line.split ";"
      
      district = District.new fields[0].to_i, fields[1].gsub(/\r\n/, "")
      districts << district
    end
    
    file.close()
    
    return districts
  end
  
  def self.load_counties
    file = File.open "data/concelhos.txt", "r:iso-8859-1:utf-8"
    
    counties = []
    
    file.each_line do |line|
      fields = line.split ";"
      
      county = County.new fields[0].to_i, fields[1].to_i, fields[2].gsub(/\r\n/, "")
      counties << county
    end
    
    file.close
    
    return counties
  end
  
  def self.load_postal_codes
    dataset_size = 4000
    file = File.open "data/todos_cp.txt", "r:iso-8859-1:utf-8"
    
    postal_codes = []
    
    file.each_line do |line|
      fields = line.split ";"
      
      postal_code = PostalCode.new fields[0].to_i, fields[1].to_i, fields[2].to_i, 
        fields[3].gsub(/\r\n/, ""), fields[4].to_i, fields[5].gsub(/\r\n/, ""), 
        fields[6].gsub(/\r\n/, ""), fields[7].gsub(/\r\n/, ""), fields[8].gsub(/\r\n/, ""),
        fields[9].gsub(/\r\n/, ""), fields[10].gsub(/\r\n/, ""), fields[11].gsub(/\r\n/, ""),
        fields[12].gsub(/\r\n/, ""), fields[13].gsub(/\r\n/, ""), fields[14].to_i,
        fields[15].to_i, fields[16].gsub(/\r\n/, "")
      postal_codes << postal_code
      
      break if postal_codes.length == dataset_size
    end
    
    file.close
    
    return postal_codes
  end
  
end