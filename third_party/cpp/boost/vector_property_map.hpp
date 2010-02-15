/*=============================================================================
Copyright (c) 2009 Trustees of Indiana University

Distributed under the Boost Software License, Version 1.0. (See accompanying
file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
=============================================================================*/
// Redirect/warning header, adapted from the version in Spirit

#include <boost/version.hpp>

#if BOOST_VERSION >= 103800
#if defined(_MSC_VER) || defined(__BORLANDC__) || defined(__DMC__)
#  pragma message ("Warning: This header is deprecated. Please use: boost/property_map/vector_property_map.hpp")
#elif defined(__GNUC__) || defined(__HP_aCC) || defined(__SUNPRO_CC) || defined(__IBMCPP__)
#  warning "This header is deprecated. Please use: boost/property_map/vector_property_map.hpp"
#endif
#endif

#include <boost/property_map/vector_property_map.hpp>
