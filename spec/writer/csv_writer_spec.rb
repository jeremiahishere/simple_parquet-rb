require 'pry'
 
RSpec.describe SimpleParquet::Writer::CsvWriter do
  it 'writes' do
    csv = File.read(File.join(File.dirname(__FILE__), '..', 'fixtures', 'hotdogs.csv'))
    writer = SimpleParquet::Writer::CsvWriter.new(csv)
    proto = writer.write

    transport = proto.trans
    output = transport.read(transport.available)

    puts output.inspect
  end
end
