#include <arrow/io/file.h>
#include <parquet/stream_writer.h>
#include <arrow/api.h>
#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <arrow/status.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

#include <iostream>
#include <string>
#include <vector>

int main(int argc, char* argv[]) {
    try {
        std::shared_ptr<arrow::io::ReadableFile> infile;
        PARQUET_ASSIGN_OR_THROW(infile, 
            arrow::io::ReadableFile::Open("test.parquet"));
        //std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        arrow::MemoryPool* pool = arrow::default_memory_pool();
        auto reader_builder = parquet::arrow::FileReaderBuilder();
        auto st = reader_builder.Open(infile);
        if (!st.ok()) {
            std::cout << "open fail" << std::endl;
        }
        std::unique_ptr<parquet::arrow::FileReader> reader;
        st = reader_builder.Build(&reader);
        if (!st.ok()) {
            std::cout << "open fail" << std::endl;
        }

        std::shared_ptr<parquet::FileMetaData> file_metadata = reader->parquet_reader()->metadata(); 
	auto* schemaDescriptor = file_metadata->schema();
        for (int i = 0; i < file_metadata->num_columns(); ++i) {
            std::string schemaName;
            // Get the Column Reader for the boolean column
	    if (schemaDescriptor->Column(i)->max_definition_level() > 1) {
		    schemaName = schemaDescriptor->Column(i)->path()->ToDotVector()[0];
	    } else {
		    schemaName = schemaDescriptor->Column(i)->name();
	    }
            std::cout << "schema name:" << schemaName << std::endl;
	}
    } catch (parquet::ParquetException& e) {
        std::cout << "Init parquet reader fail. " << e.what() << std::endl;
    }
    return 0;
        
    // parquet::StreamReader is {parquet::ParquetFileReader::Open(infile)};
    // std::shared_ptr<arrow::io::FileOutputStream> out_file;
    // PARQUET_ASSIGN_OR_THROW(out_file, arrow::io::FileOutputStream::Open("test.parquet"));

    // parquet::WriterProperties::Builder builder;

    // parquet::schema::NodeVector fields;

    // fields.push_back(parquet::schema::PrimitiveNode::Make(
    //     "name", parquet::Repetition::OPTIONAL, parquet::Type::BYTE_ARRAY,
    //     parquet::ConvertedType::UTF8));

    // fields.push_back(parquet::schema::PrimitiveNode::Make(
    //     "price", parquet::Repetition::REQUIRED, parquet::Type::FLOAT,
    //     parquet::ConvertedType::NONE, -1));

    // fields.push_back(parquet::schema::PrimitiveNode::Make(
    //     "quantity", parquet::Repetition::REQUIRED, parquet::Type::INT32,
    //     parquet::ConvertedType::INT_32, -1));
    // 
    // std::shared_ptr<parquet::schema::GroupNode> schema = std::static_pointer_cast<parquet::schema::GroupNode>(
    //   parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));

    // parquet::StreamWriter os {parquet::ParquetFileWriter::Open(out_file, schema, builder.build())};

    // for(const auto& a: get_articles()) {
    //     os << a.name << a.price << a.quantity << parquet::EndRow;
    // }
}
