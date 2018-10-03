module SimpleParquet
  module Writer
    # A list of each data page of columnar csv data
    class DataPageCollection
      include Enumerable

      # @param [ColumnarCsv] columnar_csv
      # @param [Fixnum] initial_offset Number of bytes in the file before this set of data page
      #   starts.  Used when there are multiple column chunks or to account for the leading 'PAR1'
      #   in the file
      def initialize(columnar_csv, initial_offset)
        @columnar_csv = columnar_csv
        @initial_offset = initial_offset
      end

      def headers
        @columnar_csv.headers
      end

      # yields each data page in order of the headers
      def each
        headers.each do |header|
          yield data_pages[header] if data_pages.has_key?(header)
        end
      end

      def last
        data_pages[headers.last]
      end

      def to_byte_string
        output = ""
        each do |page|
          output += page.to_byte_string
        end

        output
      end

      # Iterate over the data pages, adding up the page lengths until the header for the passed in
      #   page is found
      #
      # If the header is not found, return nil
      #
      # @param [DataPage] the_page The page to find an offset for
      # @return [Fixnum] Number of bytes from the beginning of the data pages to the beginning of
      #   the passed in data page
      def offset_for(the_page)
        offset = @initial_offset
        each do |page|
          if page.header == the_page.header
            return offset
          else
            offset += page.length
          end
        end

        return nil
      end

      # Sum off the lengths of all data pages
      #
      # Probably used in the file meta data somewhere
      def total_size
        collect(&:length).inject(0, :+)
      end

      # @param [String] header
      # @returns [DataPage] For the passed in header
      def page_for_header(header)
        data_pages[header]
      end

      # @returns [Array<DataPage>] List of the data pages for the columns in the csv
      def data_pages
        unless defined? @data_pages
          data_pages = {}
          @columnar_csv.each_column do |header, values|
            
            data_pages[header] = DataPage.new(header, values)
          end
        end

        data_pages
      end
    end
  end
end
