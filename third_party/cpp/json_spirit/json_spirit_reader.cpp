//          Copyright John W. Wilkinson 2007 - 2009.
// Distributed under the MIT License, see accompanying file LICENSE.txt

// json spirit version 4.02

#include "json_spirit_reader.h"
#include "json_spirit_value.h"

//#define BOOST_SPIRIT_THREADSAFE  // uncomment for multithreaded use, requires linking to boost.thead

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/version.hpp>

#if BOOST_VERSION >= 103800
    #include <boost/spirit/include/classic_core.hpp>
    #include <boost/spirit/include/classic_confix.hpp>
    #include <boost/spirit/include/classic_escape_char.hpp>
    #include <boost/spirit/include/classic_multi_pass.hpp>
    #include <boost/spirit/include/classic_position_iterator.hpp>
    using namespace boost::spirit::classic;
#else
    #include <boost/spirit/core.hpp>
    #include <boost/spirit/utility/confix.hpp>
    #include <boost/spirit/utility/escape_char.hpp>
    #include <boost/spirit/iterator/multi_pass.hpp>
    #include <boost/spirit/iterator/position_iterator.hpp>
    using namespace boost::spirit;
#endif

using namespace json_spirit;
using namespace std;
using namespace boost;

//

Error_position::Error_position()
:   line_( 0 )
,   column_( 0 )
{
}

Error_position::Error_position( unsigned int line, unsigned int column, const std::string& reason )
:   line_( line )
,   column_( column )
,   reason_( reason )
{
}

bool Error_position::operator==( const Error_position& lhs ) const
{
    if( this == &lhs ) return true;

    return ( reason_ == lhs.reason_ ) &&
           ( line_   == lhs.line_ ) &&
           ( column_ == lhs.column_ ); 
}

//

namespace
{
    const int_parser < int64_t >  int64_p  = int_parser < int64_t  >();
    const uint_parser< uint64_t > uint64_p = uint_parser< uint64_t >();

    template< class Iter_type >
    bool is_eq( Iter_type first, Iter_type last, const char* c_str )
    {
        for( Iter_type i = first; i != last; ++i, ++c_str )
        {
            if( *c_str == 0 ) return false;

            if( *i != *c_str ) return false;
        }

        return true;
    }

    template< class Char_type >
    Char_type hex_to_num( const Char_type c )
    {
        if( ( c >= '0' ) && ( c <= '9' ) ) return static_cast<Char_type>(c - '0');
        if( ( c >= 'a' ) && ( c <= 'f' ) ) return static_cast<Char_type>(c - 'a' + 10);
        if( ( c >= 'A' ) && ( c <= 'F' ) ) return static_cast<Char_type>(c - 'A' + 10);
        return 0;
    }

    template< class Char_type, class Iter_type >
    Char_type hex_str_to_char( Iter_type& begin )
    {
        const Char_type c1( *( ++begin ) );
        const Char_type c2( *( ++begin ) );

        return static_cast<Char_type>(( hex_to_num( c1 ) << 4 ) + hex_to_num( c2 ));
    }       

    template< class Char_type, class Iter_type >
    Char_type unicode_str_to_char( Iter_type& begin )
    {
        const Char_type c1( *( ++begin ) );
        const Char_type c2( *( ++begin ) );
        const Char_type c3( *( ++begin ) );
        const Char_type c4( *( ++begin ) );

        return static_cast<Char_type>(
               ( hex_to_num( c1 ) << 12 ) + 
               ( hex_to_num( c2 ) <<  8 ) + 
               ( hex_to_num( c3 ) <<  4 ) + 
               hex_to_num( c4 )
               );
    }

    template< class String_type >
    void append_esc_char_and_incr_iter( String_type& s, 
                                        typename String_type::const_iterator& begin, 
                                        typename String_type::const_iterator end )
    {
        typedef typename String_type::value_type Char_type;
             
        const Char_type c2( *begin );

        switch( c2 )
        {
            case 't':  s += '\t'; break;
            case 'b':  s += '\b'; break;
            case 'f':  s += '\f'; break;
            case 'n':  s += '\n'; break;
            case 'r':  s += '\r'; break;
            case '\\': s += '\\'; break;
            case '/':  s += '/';  break;
            case '"':  s += '"';  break;
            case 'x':  
            {
                if( end - begin >= 3 )  //  expecting "xHH..."
                {
                    s += hex_str_to_char< Char_type >( begin );  
                }
                break;
            }
            case 'u':  
            {
                if( end - begin >= 5 )  //  expecting "uHHHH..."
                {
                    s += unicode_str_to_char< Char_type >( begin );  
                }
                break;
            }
        }
    }

    template< class String_type >
    String_type substitute_esc_chars( typename String_type::const_iterator begin, 
                                   typename String_type::const_iterator end )
    {
        typedef typename String_type::const_iterator Iter_type;

        if( end - begin < 2 ) return String_type( begin, end );

        String_type result;
        
        result.reserve( end - begin );

        const Iter_type end_minus_1( end - 1 );

        Iter_type substr_start = begin;
        Iter_type i = begin;

        for( ; i < end_minus_1; ++i )
        {
            if( *i == '\\' )
            {
                result.append( substr_start, i );

                ++i;  // skip the '\'
             
                append_esc_char_and_incr_iter( result, i, end );

                substr_start = i + 1;
            }
        }

        result.append( substr_start, end );

        return result;
    }

    template< class String_type >
    String_type get_str_( typename String_type::const_iterator begin, 
                       typename String_type::const_iterator end )
    {
        assert( end - begin >= 2 );

        typedef typename String_type::const_iterator Iter_type;

        Iter_type str_without_quotes( ++begin );
        Iter_type end_without_quotes( --end );

        return substitute_esc_chars< String_type >( str_without_quotes, end_without_quotes );
    }

    string get_str( string::const_iterator begin, string::const_iterator end )
    {
        return get_str_< string >( begin, end );
    }

    wstring get_str( wstring::const_iterator begin, wstring::const_iterator end )
    {
        return get_str_< wstring >( begin, end );
    }
    
    template< class String_type, class Iter_type >
    String_type get_str( Iter_type begin, Iter_type end )
    {
        const String_type tmp( begin, end );  // convert multipass iterators to string iterators

        return get_str( tmp.begin(), tmp.end() );
    }

    // this class's methods get called by the spirit parse resulting
    // in the creation of a JSON object or array
    //
    // NB Iter_type could be a std::string iterator, wstring iterator, a position iterator or a multipass iterator
    //
    template< class Value_type, class Iter_type >
    class Semantic_actions 
    {
    public:

        typedef typename Value_type::Config_type Config_type;
        typedef typename Config_type::String_type String_type;
        typedef typename Config_type::Object_type Object_type;
        typedef typename Config_type::Array_type Array_type;
        typedef typename String_type::value_type Char_type;

        Semantic_actions( Value_type& value )
        :   value_( value )
        ,   current_p_( 0 )
        {
        }

        void begin_obj( Char_type c )
        {
            assert( c == '{' );

            begin_compound< Object_type >();
        }

        void end_obj( Char_type c )
        {
            assert( c == '}' );

            end_compound();
        }

        void begin_array( Char_type c )
        {
            assert( c == '[' );
     
            begin_compound< Array_type >();
        }

        void end_array( Char_type c )
        {
            assert( c == ']' );

            end_compound();
        }

        void new_name( Iter_type begin, Iter_type end )
        {
            assert( current_p_->type() == obj_type );

            name_ = get_str< String_type >( begin, end );
        }

        void new_str( Iter_type begin, Iter_type end )
        {
            add_to_current( get_str< String_type >( begin, end ) );
        }

        void new_true( Iter_type begin, Iter_type end )
        {
            assert( is_eq( begin, end, "true" ) );

            add_to_current( true );
        }

        void new_false( Iter_type begin, Iter_type end )
        {
            assert( is_eq( begin, end, "false" ) );

            add_to_current( false );
        }

        void new_null( Iter_type begin, Iter_type end )
        {
            assert( is_eq( begin, end, "null" ) );

            add_to_current( Value_type() );
        }

        void new_int( int64_t i )
        {
            add_to_current( i );
        }

        void new_uint64( uint64_t ui )
        {
            add_to_current( ui );
        }

        void new_real( double d )
        {
            add_to_current( d );
        }

    private:

        Semantic_actions& operator=( const Semantic_actions& ); 
                                    // to prevent "assignment operator could not be generated" warning

        Value_type* add_first( const Value_type& value )
        {
            assert( current_p_ == 0 );

            value_ = value;
            current_p_ = &value_;
            return current_p_;
        }

        template< class Array_or_obj >
        void begin_compound()
        {
            if( current_p_ == 0 )
            {
                add_first( Array_or_obj() );
            }
            else
            {
                stack_.push_back( current_p_ );

                Array_or_obj new_array_or_obj;   // avoid copy by building new array or object in place

                current_p_ = add_to_current( new_array_or_obj );
            }
        }

        void end_compound()
        {
            if( current_p_ != &value_ )
            {
                current_p_ = stack_.back();
                
                stack_.pop_back();
            }    
        }

        Value_type* add_to_current( const Value_type& value )
        {
            if( current_p_ == 0 )
            {
                return add_first( value );
            }
            else if( current_p_->type() == array_type )
            {
                current_p_->get_array().push_back( value );

                return &current_p_->get_array().back(); 
            }
            
            assert( current_p_->type() == obj_type );

            return &Config_type::add( current_p_->get_obj(), name_, value );
        }

        Value_type& value_;             // this is the object or array that is being created
        Value_type* current_p_;         // the child object or array that is currently being constructed

        vector< Value_type* > stack_;   // previous child objects and arrays

        String_type name_;              // of current name/value pair
    };

    template< typename Iter_type >
    void throw_error( position_iterator< Iter_type > i, const std::string& reason )
    {
        throw Error_position( i.get_position().line, i.get_position().column, reason );
    }

    template< typename Iter_type >
    void throw_error( Iter_type i, const std::string& reason )
    {
       throw reason;
    }

    // the spirit grammer 
    //
    template< class Value_type, class Iter_type >
    class Json_grammer : public grammar< Json_grammer< Value_type, Iter_type > >
    {
    public:

        typedef Semantic_actions< Value_type, Iter_type > Semantic_actions_t;

        Json_grammer( Semantic_actions_t& semantic_actions )
        :   actions_( semantic_actions )
        {
        }

        static void throw_not_value( Iter_type begin, Iter_type end )
        {
    	    throw_error( begin, "not a value" );
        }

        static void throw_not_array( Iter_type begin, Iter_type end )
        {
    	    throw_error( begin, "not an array" );
        }

        static void throw_not_object( Iter_type begin, Iter_type end )
        {
    	    throw_error( begin, "not an object" );
        }

        static void throw_not_pair( Iter_type begin, Iter_type end )
        {
    	    throw_error( begin, "not a pair" );
        }

        static void throw_not_colon( Iter_type begin, Iter_type end )
        {
    	    throw_error( begin, "no colon in pair" );
        }

        static void throw_not_string( Iter_type begin, Iter_type end )
        {
    	    throw_error( begin, "not a string" );
        }

        template< typename ScannerT >
        class definition
        {
        public:

            definition( const Json_grammer& self )
            {
                typedef typename Value_type::String_type::value_type Char_type;

                // first we convert the semantic action class methods to functors with the 
                // parameter signature expected by spirit

                typedef function< void( Char_type )            > Char_action;
                typedef function< void( Iter_type, Iter_type ) > Str_action;
                typedef function< void( double )               > Real_action;
                typedef function< void( int64_t )              > Int_action;
                typedef function< void( uint64_t )             > Uint64_action;

                Char_action   begin_obj  ( bind( &Semantic_actions_t::begin_obj,   &self.actions_, _1 ) );
                Char_action   end_obj    ( bind( &Semantic_actions_t::end_obj,     &self.actions_, _1 ) );
                Char_action   begin_array( bind( &Semantic_actions_t::begin_array, &self.actions_, _1 ) );
                Char_action   end_array  ( bind( &Semantic_actions_t::end_array,   &self.actions_, _1 ) );
                Str_action    new_name   ( bind( &Semantic_actions_t::new_name,    &self.actions_, _1, _2 ) );
                Str_action    new_str    ( bind( &Semantic_actions_t::new_str,     &self.actions_, _1, _2 ) );
                Str_action    new_true   ( bind( &Semantic_actions_t::new_true,    &self.actions_, _1, _2 ) );
                Str_action    new_false  ( bind( &Semantic_actions_t::new_false,   &self.actions_, _1, _2 ) );
                Str_action    new_null   ( bind( &Semantic_actions_t::new_null,    &self.actions_, _1, _2 ) );
                Real_action   new_real   ( bind( &Semantic_actions_t::new_real,    &self.actions_, _1 ) );
                Int_action    new_int    ( bind( &Semantic_actions_t::new_int,     &self.actions_, _1 ) );
                Uint64_action new_uint64 ( bind( &Semantic_actions_t::new_uint64,  &self.actions_, _1 ) );

                // actual grammer

                json_
                    = value_ | eps_p[ &throw_not_value ]
                    ;

                value_
                    = string_[ new_str ] 
                    | number_ 
                    | object_ 
                    | array_ 
                    | str_p( "true" ) [ new_true  ] 
                    | str_p( "false" )[ new_false ] 
                    | str_p( "null" ) [ new_null  ]
                    ;

                object_ 
                    = ch_p('{')[ begin_obj ]
                    >> !members_
                    >> ( ch_p('}')[ end_obj ] | eps_p[ &throw_not_object ] )
                    ;

                members_
                    = pair_ >> *( ',' >> pair_ )
                    ;

                pair_
                    = string_[ new_name ]
                    >> ( ':' | eps_p[ &throw_not_colon ] )
                    >> ( value_ | eps_p[ &throw_not_value ] )
                    ;

                array_
                    = ch_p('[')[ begin_array ]
                    >> !elements_
                    >> ( ch_p(']')[ end_array ] | eps_p[ &throw_not_array ] )
                    ;

                elements_
                    = value_ >> *( ',' >> value_ )
                    ;

                string_ 
                    = lexeme_d // this causes white space inside a string to be retained
                      [
                          confix_p
                          ( 
                              '"', 
                              *lex_escape_ch_p,
                              '"'
                          ) 
                      ]
                    ;

                number_
                    = strict_real_p[ new_real   ] 
                    | int64_p      [ new_int    ]
                    | uint64_p     [ new_uint64 ]
                    ;
            }

            rule< ScannerT > json_, object_, members_, pair_, array_, elements_, value_, string_, number_;

            const rule< ScannerT >& start() const { return json_; }
        };

    private:

        Json_grammer& operator=( const Json_grammer& ); // to prevent "assignment operator could not be generated" warning

        Semantic_actions_t& actions_;
    };

    template< class Iter_type, class Value_type >
    Iter_type read_range_or_throw( Iter_type begin, Iter_type end, Value_type& value )
    {
        Semantic_actions< Value_type, Iter_type > semantic_actions( value );
     
        const parse_info< Iter_type > info = parse( begin, end, 
                                                    Json_grammer< Value_type, Iter_type >( semantic_actions ), 
                                                    space_p );

        if( !info.hit )
        {
            assert( false ); // in theory exception should already have been thrown
            throw_error( info.stop, "error" );
        }

        return info.stop;
    }

    template< class Iter_type, class Value_type >
    void add_posn_iter_and_read_range_or_throw( Iter_type begin, Iter_type end, Value_type& value )
    {
        typedef position_iterator< Iter_type > Posn_iter_t;

        const Posn_iter_t posn_begin( begin, end );
        const Posn_iter_t posn_end( end, end );
     
        read_range_or_throw( posn_begin, posn_end, value );
    }

    template< class Iter_type, class Value_type >
    bool read_range( Iter_type& begin, Iter_type end, Value_type& value )
    {
        try
        {
            begin = read_range_or_throw( begin, end, value );

            return true;
        }
        catch( ... )
        {
            return false;
        }
    }

    template< class String_type, class Value_type >
    void read_string_or_throw( const String_type& s, Value_type& value )
    {
        add_posn_iter_and_read_range_or_throw( s.begin(), s.end(), value );
    }

    template< class String_type, class Value_type >
    bool read_string( const String_type& s, Value_type& value )
    {
        typename String_type::const_iterator begin = s.begin();

        return read_range( begin, s.end(), value );
    }

    template< class Istream_type >
    struct Multi_pass_iters
    {
        typedef typename Istream_type::char_type Char_type;
        typedef istream_iterator< Char_type, Char_type > istream_iter;
        typedef multi_pass< istream_iter > multi_pass_iter;

        Multi_pass_iters( Istream_type& is )
        {
            is.unsetf( ios::skipws );

            begin_ = make_multi_pass( istream_iter( is ) );
            end_   = make_multi_pass( istream_iter() );
        }

        multi_pass_iter begin_;
        multi_pass_iter end_;
    };

    template< class Istream_type, class Value_type >
    bool read_stream( Istream_type& is, Value_type& value )
    {
        Multi_pass_iters< Istream_type > mp_iters( is );

        return read_range( mp_iters.begin_, mp_iters.end_, value );
    }

    template< class Istream_type, class Value_type >
    void read_stream_or_throw( Istream_type& is, Value_type& value )
    {
        const Multi_pass_iters< Istream_type > mp_iters( is );

        add_posn_iter_and_read_range_or_throw( mp_iters.begin_, mp_iters.end_, value );
    }
}

bool json_spirit::read( const std::string& s, Value& value )
{
    return read_string( s, value );
}

void json_spirit::read_or_throw( const std::string& s, Value& value )
{
    read_string_or_throw( s, value );
}

bool json_spirit::read( std::istream& is, Value& value )
{
    return read_stream( is, value );
}

void json_spirit::read_or_throw( std::istream& is, Value& value )
{
    read_stream_or_throw( is, value );
}

bool json_spirit::read( std::string::const_iterator& begin, std::string::const_iterator end, Value& value )
{
    return read_range( begin, end, value );
}

void json_spirit::read_or_throw( std::string::const_iterator& begin, std::string::const_iterator end, Value& value )
{
    begin = read_range_or_throw( begin, end, value );
}

#ifndef BOOST_NO_STD_WSTRING

bool json_spirit::read( const std::wstring& s, wValue& value )
{
    return read_string( s, value );
}

void json_spirit::read_or_throw( const std::wstring& s, wValue& value )
{
    read_string_or_throw( s, value );
}

bool json_spirit::read( std::wistream& is, wValue& value )
{
    return read_stream( is, value );
}

void json_spirit::read_or_throw( std::wistream& is, wValue& value )
{
    read_stream_or_throw( is, value );
}

bool json_spirit::read( std::wstring::const_iterator& begin, std::wstring::const_iterator end, wValue& value )
{
    return read_range( begin, end, value );
}

void json_spirit::read_or_throw( std::wstring::const_iterator& begin, std::wstring::const_iterator end, wValue& value )
{
    begin = read_range_or_throw( begin, end, value );
}

#endif

bool json_spirit::read( const std::string& s, mValue& value )
{
    return read_string( s, value );
}

void json_spirit::read_or_throw( const std::string& s, mValue& value )
{
    read_string_or_throw( s, value );
}

bool json_spirit::read( std::istream& is, mValue& value )
{
    return read_stream( is, value );
}

void json_spirit::read_or_throw( std::istream& is, mValue& value )
{
    read_stream_or_throw( is, value );
}

bool json_spirit::read( std::string::const_iterator& begin, std::string::const_iterator end, mValue& value )
{
    return read_range( begin, end, value );
}

void json_spirit::read_or_throw( std::string::const_iterator& begin, std::string::const_iterator end, mValue& value )
{
    begin = read_range_or_throw( begin, end, value );
}

#ifndef BOOST_NO_STD_WSTRING

bool json_spirit::read( const std::wstring& s, wmValue& value )
{
    return read_string( s, value );
}

void json_spirit::read_or_throw( const std::wstring& s, wmValue& value )
{
    read_string_or_throw( s, value );
}

bool json_spirit::read( std::wistream& is, wmValue& value )
{
    return read_stream( is, value );
}

void json_spirit::read_or_throw( std::wistream& is, wmValue& value )
{
    read_stream_or_throw( is, value );
}

bool json_spirit::read( std::wstring::const_iterator& begin, std::wstring::const_iterator end, wmValue& value )
{
    return read_range( begin, end, value );
}

void json_spirit::read_or_throw( std::wstring::const_iterator& begin, std::wstring::const_iterator end, wmValue& value )
{
    begin = read_range_or_throw( begin, end, value );
}

#endif
