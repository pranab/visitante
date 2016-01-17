#!/usr/bin/ruby

require '../lib/util.rb'      

count = ARGV[0].to_i
length = ARGV[1].to_i

idGen = IdGenerator.new
categories = ["supplements", "vitamins", "diet", "beauty", "food", "home", "pets", "baby", "herbs", "drugs"]


1.upto count do
	prod_id = idGen.generate(length)
	category = categories[rand(categories.length)]
	rating = 1 + rand(5)
	puts "#{prod_id}\t#{rating}\t#{category}"
end
