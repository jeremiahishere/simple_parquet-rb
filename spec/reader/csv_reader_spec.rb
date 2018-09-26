require 'pry'
 
RSpec.describe SimpleParquet::Reader::CsvReader do
  it 'reads' do
    parquet = File.read(File.join(File.dirname(__FILE__), '..', 'fixtures', 'hotdogs.parquet'))
    reader = SimpleParquet::Reader::CsvReader.new(parquet)

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
