module SimpleParquet
  module Writer
    RSpec.describe ParquetDocument do
      let(:raw_csv) do
        File.read(File.join(File.dirname(__FILE__), '..', 'fixtures', 'hotdogs.csv'))
      end

      describe '.to_byte_string' do
        it 'writes a readable parquet file from a csv string' do
          doc = ParquetDocument.new(raw_csv)

          # this feels fragile, giving up for now
          expected_value = "PAR1\x15\x00\x15\xA8\x01\x15\xA8\x01,\x15\x0E\x15\x00\x15\x00\x15\x00\x00\x00\x06\x00\x00\x00hotdog\t\x00\x00\x00bratwurst\b\x00\x00\x00kielbasa\a\x00\x00\x00chorizo\n\x00\x00\x00liverwurst\t\x00\x00\x00andouille\a\x00\x00\x00bologna\x15\x00\x15\x80\x01\x15\x80\x01,\x15\x0E\x15\x00\x15\x00\x15\x00\x00\x00\x04\x00\x00\x00good\a\x00\x00\x00just ok\x05\x00\x00\x00smoky\x05\x00\x00\x00ariba\x05\x00\x00\x00irony\x05\x00\x00\x00sweet\x05\x00\x00\x00pasty\x15\x02\x19,\x15\f%\x00\x18\aSausage%\x00L\x1C\x00\x00\x00\x15\f%\x00\x18\x05Taste%\x00L\x1C\x00\x00\x00\x16\x0E\x19\x1C\x19,&\b\x1C\x15\f\x19\x15\x00\x19\x18\aSausage\x15\x00\x16\x0E\x16\xCE\x01\x16\xCE\x01&\b\x00\x00&\xD6\x01\x1C\x15\f\x19\x15\x00\x19\x18\x05Taste\x15\x00\x16\x0E\x16\xA6\x01\x16\xA6\x01&\xD6\x01\x00\x00\x16\xF4\x02\x16\x0E&\b\x00(\x03O2O\x00~\x00\x00\x00PAR1".force_encoding(Encoding::BINARY)

          expect(doc.to_byte_string).to eq(expected_value)
        end

        it 'writes a string that the reader can read' do
          doc = ParquetDocument.new(raw_csv)

          reader = SimpleParquet::Reader::Parser.new(doc.to_byte_string)

          output = []
          reader.each do |part|
            output << part
          end

          expect(output.size).to eq(2)
          expect(output[0].rows).to eq(["Sausage", "hotdog", "bratwurst", "kielbasa", "chorizo", "liverwurst", "andouille", "bologna"])
          expect(output[1].rows).to eq(["Taste", "good", "just ok", "smoky", "ariba", "irony", "sweet", "pasty"])
        end
      end
    end
  end
end
