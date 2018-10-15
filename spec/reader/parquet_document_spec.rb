module SimpleParquet
  module Reader
    RSpec.describe ParquetDocument do
      let(:raw_parquet) do
        File.read(File.join(File.dirname(__FILE__), '..', 'fixtures', 'hotdogs.parquet'))
      end

      let(:expected_csv) do
        File.read(File.join(File.dirname(__FILE__), '..', 'fixtures', 'hotdogs.csv'))
      end

      describe '.csv' do
        it 'converts raw parquet into a csv object' do
          pd = ParquetDocument.new(raw_parquet)

          expect(pd.csv.to_s).to eq(expected_csv)
        end
      end
    end
  end
end
