#!/usr/bin/ruby

require '../lib/util.rb'      

count = ARGV[0].to_i
length = ARGV[1].to_i

id_gen = IdGenerator.new

1.upto count do
	user_id = id_gen.generate(length)
	puts "#{user_id}"
end
