module SimpleParquet
  module Reader
    RSpec.describe FileMetaData do
      let(:raw_parquet) do
          StringIO.new("\x15\x02\x19,\x15\f%\x00\x18\aSausage%\x00L\x1C\x00\x00\x00\x15\f%\x00\x18\x05Taste%\x00L\x1C\x00\x00\x00\x16\x0E\x19\x1C\x19,&\b\x1C\x15\f\x19\x15\x00\x19\x18\aSausage\x15\x00\x16\x0E\x16\xCE\x01\x16\xCE\x01&\b\x00\x00&\xD6\x01\x1C\x15\f\x19\x15\x00\x19\x18\x05Taste\x15\x00\x16\x0E\x16\xA6\x01\x16\xA6\x01&\xD6\x01\x00\x00\x16\xF4\x02\x16\x0E&\b\x00(\x03O2O\x00~\x00\x00\x00PAR1")
      end

      let(:parquet_special_string) do
        "PAR1"
      end

      describe '.fmd' do
        it 'parses a file meta data out of the raw parquet' do
          fmd = FileMetaData.new(raw_parquet, parquet_special_string)

          the_fmd = fmd.fmd
          expect(the_fmd).to be_a(::FileMetaData)
          expect(the_fmd.num_rows).to eq(7)
        end
      end

      describe '.fmd_size' do
        it 'returns the start offset plus the size of the special string' do
          fmd = FileMetaData.new(raw_parquet, parquet_special_string)

          expect(fmd.fmd_size).to eq(126)
        end
      end

      describe '.fmd_start_offset' do
        it 'returns the int 32 stored at the fmd size offset location' do
          fmd = FileMetaData.new(raw_parquet, parquet_special_string)

          expect(fmd.fmd_start_offset).to eq(-134)
        end
      end

      describe '.fmd_size_offset' do
        it 'returns the number of bytes from the end of the file where the offset is located' do
          fmd = FileMetaData.new(raw_parquet, parquet_special_string)

          # size of the special string plus the size of an int 32
          expect(fmd.fmd_size_offset).to eq(-8)
        end
      end
    end
  end
end
