require 'pry'
 
RSpec.describe SimpleParquet::Writer::CsvWriter do
  it 'writes' do
    csv = File.read(File.join(File.dirname(__FILE__), '..', 'fixtures', 'hotdogs.csv'))
    writer = SimpleParquet::Writer::CsvWriter.new(csv)
    proto = writer.write

    fmd = FileMetaData.new
    fmd.read(proto)
    puts fmd.inspect

    expect(fmd.version).to be 1
    expect(fmd.num_rows).to be 7
  end
end
