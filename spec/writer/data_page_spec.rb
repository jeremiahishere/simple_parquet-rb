module SimpleParquet
  module Writer
    RSpec.describe DataPage do
      let(:header) { 'Sausages' }
      let(:values) { ['hotdogs', 'bratwurst'] }

      describe '.page_header' do
        it 'includes the page data length' do
          dp = DataPage.new(header, values)
          allow(dp).to receive(:page_data) { "<page data>" }

          page_header = dp.page_header

          expect(page_header.uncompressed_page_size).to eq(11)
          expect(page_header.compressed_page_size).to eq(11)
        end

        it 'includes the number of values' do
          dp = DataPage.new(header, values)
          page_header = dp.page_header

          expect(page_header.data_page_header.num_values).to eq(2)
        end
      end

      describe '.page_data' do
        it 'encodes and returns the values in the proper <length of string><string> format' do
          dp = DataPage.new(header, ['hello'])

          expect(dp.page_data).to eq("\x05\x00\x00\x00hello")
        end

        it 'encodes the length of the string in 4 byte (i32) little endian format' do
          dp = DataPage.new(header, ['hello'])

          expect(dp.page_data).to include([5].pack("l<"))
        end
      end

      describe '.to_byte_string' do
        it 'combines the page header and the page data into a single string' do
          dp = DataPage.new(header, values)
          allow(dp).to receive(:page_header_byte_string) { "<page header>" }
          allow(dp).to receive(:page_data) { "<page data>" }

          expect(dp.to_byte_string).to eq("<page header><page data>")
        end

        it 'renders the passed in data in encoded form' do
          # this test sort of sucks but it is better than not having it, I think
          dp = DataPage.new(header, values)

          expect(dp.to_byte_string).to eq("\x15\x00\x150\x150,\x15\x04\x15\x00\x15\x00\x15\x00\x00\x00\a\x00\x00\x00hotdogs\t\x00\x00\x00bratwurst")
        end
      end

      describe '.length' do
        it 'returns the length of the encoded data' do
          dp = DataPage.new(header, values)
          allow(dp).to receive(:to_byte_string) { "sample byte string" }

          expect(dp.length).to eq(18)
        end
      end
    end
  end
end
