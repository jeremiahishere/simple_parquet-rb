module SimpleParquet
  module Writer
    module Configurator
      def self.configurate(klass, *args)
        instance = klass.new(args)

        yield(instance)

        instance
      end
    end
  end
end
