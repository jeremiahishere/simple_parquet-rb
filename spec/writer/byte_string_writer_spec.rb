module SimpleParquet
  module Writer
    RSpec.describe ByteStringWriter do
      it 'converts a string to a byte string' do
        writer = ByteStringWriter.new("hello")

        expect(writer.to_byte_string).to eq('hello')
      end

      it 'converts a fixnum to a byte string' do
        writer = ByteStringWriter.new(42)

        expect(writer.to_byte_string).to eq("*\x00\x00\x00")

        # should be little endian
        expect(writer.to_byte_string).to eq([42].pack("l<"))
      end

      it 'converts a thrift object to a byte string' do
        data_page_header = Configurator.data_page_header_with_defaults({
          num_values: 1
        })
        writer = ByteStringWriter.new(data_page_header)

        # semi-fragile test may break if parquet updates and the format of data page header changes
        expect(writer.to_byte_string).to eq("\x15\x02\x15\x00\x15\x00\x15\x00\x00")
      end

      it 'converts an array of strings to a byte string' do
        writer = ByteStringWriter.new(['hello', 'world'])

        expect(writer.to_byte_string).to eq("\x05\x00\x00\x00hello\x05\x00\x00\x00world")
      end
    end
  end
end
