module SimpleParquet
  module Reader
    RSpec.describe DataPageCollection do
      let(:row_group) do
        Support::Configurator.row_group_with_defaults({
          # totally cheating and reading the same string twice
          # this is definitely not realistic
          columns: [
            { meta_data: { data_page_offset: 0 }},
            { meta_data: { data_page_offset: 0 }}
          ]
        })
      end

      let(:raw_parquet) do
        # rendered data page header and column with data ['hotdogs', 'bratwurst']
        StringIO.new("\x15\x00\x150\x150,\x15\x04\x15\x00\x15\x00\x15\x00\x00\x00\a\x00\x00\x00hotdogs\t\x00\x00\x00bratwurst")
      end

      describe '.data_pages' do
        it 'reads the data pages from the parquet document' do
          dpc = DataPageCollection.new(raw_parquet, row_group)

          dpc.data_pages.each do |data_page|
            expect(data_page.page_data).to eq(['hotdogs', 'bratwurst'])
          end
        end
      end
    end
  end
end
