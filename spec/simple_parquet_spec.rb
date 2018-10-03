RSpec.describe SimpleParquet do
  it "has a version number" do
    expect(SimpleParquet::VERSION).not_to be nil
  end

  it "can write and read parquet files" do
    csv = File.read(File.join(File.dirname(__FILE__), 'fixtures', 'hotdogs.csv'))
    writer = SimpleParquet::Writer::ParquetDocument.new(csv)

    output = writer.to_byte_string

    `rm -f /tmp/test.parquet`
    p output
    File.open("/tmp/test.parquet", "w") do |file|
      file.puts output
    end

    raw = File.read("/tmp/test.parquet")

    reader = SimpleParquet::Reader::CsvReader.new(raw)

    output_io = StringIO.new
    csv = reader.read(output_io)

    expected_csv = <<-EXPECTED_CSV
Sausage,Taste
hotdog,good
bratwurst,just ok
kielbasa,smoky
chorizo,ariba
liverwurst,irony
andouille,sweet
bologna,pasty
    EXPECTED_CSV

    expect(csv).to eq(expected_csv)
  end
end
