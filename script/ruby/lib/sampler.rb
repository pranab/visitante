
op = ARGV[0]
file = ARGV[1]
numLines = ARGV[2].to_i
percent = ARGV[3].to_i


class Sampler

	def subSample(file, numLines, percent)
		numSample = (numLines * percent) / 100
		sampInterval = numLines / numSample
		
		File.open(file, "r") do |infile|
			count = 0
			skip = sampInterval - 2 + rand(2)
			while ((line = infile.gets) && (count < numSample))
				if (skip > 0)
					skip = skip -1
				else
					puts line
					skip = sampInterval - 2 + rand(2)
					count = count + 1
				end
			end
		end
	end

end 

sampler = Sampler.new
if (op == "ss")
	sampler.subSample(file, numLines, percent)
end
