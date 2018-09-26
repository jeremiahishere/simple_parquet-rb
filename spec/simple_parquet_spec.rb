RSpec.describe SimpleParquet do
  it "has a version number" do
    expect(SimpleParquet::VERSION).not_to be nil
  end

  it "can write and read parquet files" do
    csv = File.read(File.join(File.dirname(__FILE__), 'fixtures', 'hotdogs.csv'))
    writer = SimpleParquet::Writer::CsvWriter.new(csv)
    proto = writer.write

    transport = proto.trans
    output = transport.read(transport.available)


    `rm -f /tmp/test.parquet`
    p output
    File.open("/tmp/test.parquet", "w") do |file|
      file.puts output
    end

    raw = File.read("/tmp/test.parquet")

    reader = SimpleParquet::Reader::CsvReader.new(raw)

    output_io = StringIO.new
    csv = reader.read(output_io)

    p csv


  end
end
