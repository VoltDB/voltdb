// Copyright 2004, 2005 The Trustees of Indiana University.

// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

//  Authors: Douglas Gregor
//           Andrew Lumsdaine
#ifndef BOOST_GRAPH_FRUCHTERMAN_REINGOLD_FORCE_DIRECTED_LAYOUT_HPP
#define BOOST_GRAPH_FRUCHTERMAN_REINGOLD_FORCE_DIRECTED_LAYOUT_HPP

#include <boost/config/no_tr1/cmath.hpp>
#include <boost/graph/graph_traits.hpp>
#include <boost/graph/named_function_params.hpp>
#include <boost/graph/iteration_macros.hpp>
#include <boost/graph/topology.hpp> // For topology concepts
#include <vector>
#include <list>
#include <algorithm> // for std::min and std::max
#include <numeric> // for std::accumulate
#include <cmath> // for std::sqrt and std::fabs
#include <functional>

namespace boost {

  bool vertex_migration = false;

struct square_distance_attractive_force {
  template<typename Graph, typename T>
  T
  operator()(typename graph_traits<Graph>::edge_descriptor,
             T k,
             T d,
             const Graph&) const
  {
    return d * d / k;
  }
};

struct square_distance_repulsive_force {
  template<typename Graph, typename T>
  T
  operator()(typename graph_traits<Graph>::vertex_descriptor,
             typename graph_traits<Graph>::vertex_descriptor,
             T k,
             T d,
             const Graph&) const
  {
    return k * k / d;
  }
};

template<typename T>
struct linear_cooling {
  typedef T result_type;

  linear_cooling(std::size_t iterations)
    : temp(T(iterations) / T(10)), step(0.1) { }

  linear_cooling(std::size_t iterations, T temp)
    : temp(temp), step(temp / T(iterations)) { }

  T operator()()
  {
    T old_temp = temp;
    temp -= step;
    if (temp < T(0)) temp = T(0);
    return old_temp;
  }

 private:
  T temp;
  T step;
};

struct all_force_pairs
{
  template<typename Graph, typename ApplyForce >
  void operator()(const Graph& g, ApplyForce apply_force)
  {
    typedef typename graph_traits<Graph>::vertex_iterator vertex_iterator;
    vertex_iterator v, end;
    for (tie(v, end) = vertices(g); v != end; ++v) {
      vertex_iterator u = v;
      for (++u; u != end; ++u) {
        apply_force(*u, *v);
        apply_force(*v, *u);
      }
    }
  }
};

template<typename Topology, typename PositionMap>
struct grid_force_pairs
{
  typedef typename property_traits<PositionMap>::value_type Point;
  BOOST_STATIC_ASSERT (Point::dimensions == 2);
  typedef typename Topology::point_difference_type point_difference_type;

  template<typename Graph>
  explicit
  grid_force_pairs(const Topology& topology,
                   const Point& origin, const point_difference_type& extent, 
                   PositionMap position, const Graph& g)
    : topology(topology), extent(extent), origin(origin), position(position)
  {
    two_k = 2. * this->topology.volume(this->extent) / std::sqrt((double)num_vertices(g));
  }

  template<typename Graph, typename ApplyForce >
  void operator()(const Graph& g, ApplyForce apply_force)
  {
    typedef typename graph_traits<Graph>::vertex_iterator vertex_iterator;
    typedef typename graph_traits<Graph>::vertex_descriptor vertex_descriptor;
    typedef std::list<vertex_descriptor> bucket_t;
    typedef std::vector<bucket_t> buckets_t;

    std::size_t columns = std::size_t(extent[0] / two_k + 1.);
    std::size_t rows = std::size_t(extent[1] / two_k + 1.);
    buckets_t buckets(rows * columns);
    vertex_iterator v, v_end;
    for (tie(v, v_end) = vertices(g); v != v_end; ++v) {
      std::size_t column = std::size_t((position[*v][0] + extent[0] / 2) / two_k);
      std::size_t row    = std::size_t((position[*v][1] + extent[1] / 2) / two_k);

      if (column >= columns) column = columns - 1;
      if (row >= rows) row = rows - 1;
      buckets[row * columns + column].push_back(*v);
    }

    for (std::size_t row = 0; row < rows; ++row)
      for (std::size_t column = 0; column < columns; ++column) {
        bucket_t& bucket = buckets[row * columns + column];
        typedef typename bucket_t::iterator bucket_iterator;
        for (bucket_iterator u = bucket.begin(); u != bucket.end(); ++u) {
          // Repulse vertices in this bucket
          bucket_iterator v = u;
          for (++v; v != bucket.end(); ++v) {
            apply_force(*u, *v);
            apply_force(*v, *u);
          }

          std::size_t adj_start_row = row == 0? 0 : row - 1;
          std::size_t adj_end_row = row == rows - 1? row : row + 1;
          std::size_t adj_start_column = column == 0? 0 : column - 1;
          std::size_t adj_end_column = column == columns - 1? column : column + 1;
          for (std::size_t other_row = adj_start_row; other_row <= adj_end_row;
               ++other_row)
            for (std::size_t other_column = adj_start_column;
                 other_column <= adj_end_column; ++other_column)
              if (other_row != row || other_column != column) {
                // Repulse vertices in this bucket
                bucket_t& other_bucket
                  = buckets[other_row * columns + other_column];
                for (v = other_bucket.begin(); v != other_bucket.end(); ++v) {
                  double dist = topology.distance(position[*u], position[*v]);
                  if (dist < two_k) apply_force(*u, *v);
                }
              }
        }
      }
  }

 private:
  const Topology& topology;
  point_difference_type extent;
  Point origin;
  PositionMap position;
  double two_k;
};

template<typename PositionMap, typename Topology, typename Graph>
inline grid_force_pairs<Topology, PositionMap>
make_grid_force_pairs
  (const Topology& topology,
   typename Topology::point_type const& origin,
   typename Topology::point_difference_type const& extent,
   const PositionMap& position, const Graph& g)
{ return grid_force_pairs<Topology, PositionMap>(topology, origin, extent, position, g); }

template<typename Graph, typename PositionMap, typename Topology>
void
scale_graph(const Graph& g, PositionMap position, const Topology& topology,
            typename Topology::point_type upper_left, typename Topology::point_type lower_right)
{
  if (num_vertices(g) == 0) return;

  typedef typename Topology::point_type Point;
  typedef typename Topology::point_difference_type point_difference_type;

  // Find min/max ranges
  Point min_point = position[*vertices(g).first], max_point = min_point;
  BGL_FORALL_VERTICES_T(v, g, Graph) {
    min_point = topology.pointwise_min(min_point, position[v]);
    max_point = topology.pointwise_max(max_point, position[v]);
  }

  Point old_origin = topology.move_position_toward(min_point, 0.5, max_point);
  Point new_origin = topology.move_position_toward(upper_left, 0.5, lower_right);
  point_difference_type old_size = topology.difference(max_point, min_point);
  point_difference_type new_size = topology.difference(lower_right, upper_left);

  // Scale to bounding box provided
  BGL_FORALL_VERTICES_T(v, g, Graph) {
    point_difference_type relative_loc = topology.difference(position[v], old_origin);
    relative_loc = (relative_loc / old_size) * new_size;
    position[v] = topology.adjust(new_origin, relative_loc);
  }
}

namespace detail {
  template<typename Topology>
  void 
  maybe_jitter_point(const Topology& topology,
                     typename Topology::point_type& p1, const typename Topology::point_type& p2,
                     typename Topology::point_type origin, typename Topology::point_difference_type extent)
  {
    double too_close = topology.norm(extent) / 10000.;
    if (topology.distance(p1, p2) < too_close) {
      p1 = topology.move_position_toward(p1, 1./200, topology.random_point());
    }
  }

  template<typename Topology, typename PositionMap, typename DisplacementMap,
           typename RepulsiveForce, typename Graph>
  struct fr_apply_force
  {
    typedef typename graph_traits<Graph>::vertex_descriptor vertex_descriptor;
    typedef typename Topology::point_type Point;
    typedef typename Topology::point_difference_type PointDiff;

    fr_apply_force(const Topology& topology,
                   const PositionMap& position,
                   const DisplacementMap& displacement,
                   Point origin, PointDiff extent,
                   RepulsiveForce repulsive_force, double k, const Graph& g)
      : topology(topology), position(position), displacement(displacement), origin(origin),
        extent(extent), repulsive_force(repulsive_force), k(k), g(g)
    { }

    void operator()(vertex_descriptor u, vertex_descriptor v)
    {
      if (u != v) {
        // When the vertices land on top of each other, move the
        // first vertex away from the boundaries.
        maybe_jitter_point(topology, position[u], position[v], origin, extent);

        double dist = topology.distance(position[u], position[v]);
        if (dist == 0.) {
          for (std::size_t i = 0; i < Point::dimensions; ++i) {
            displacement[v][i] += 0.01;
          }
        } else {
          double fr = repulsive_force(u, v, k, dist, g);
          typename Topology::point_difference_type dispv = displacement[v];
          dispv += (fr / dist) * topology.difference(position[v], position[u]);
        }
      }
    }

  private:
    const Topology& topology;
    PositionMap position;
    DisplacementMap displacement;
    Point origin;
    PointDiff extent;
    RepulsiveForce repulsive_force;
    double k;
    const Graph& g;
  };

} // end namespace detail

template<typename Topology, typename Graph, typename PositionMap, 
         typename AttractiveForce, typename RepulsiveForce,
         typename ForcePairs, typename Cooling, typename DisplacementMap>
void
fruchterman_reingold_force_directed_layout
 (const Graph&    g,
  PositionMap     position,
  const Topology& topology,
  typename Topology::point_type const& origin,
  typename Topology::point_difference_type const& extent,
  AttractiveForce attractive_force,
  RepulsiveForce  repulsive_force,
  ForcePairs      force_pairs,
  Cooling         cool,
  DisplacementMap displacement)
{
  typedef typename Topology::point_type Point;
  typedef typename graph_traits<Graph>::vertex_iterator   vertex_iterator;
  typedef typename graph_traits<Graph>::vertex_descriptor vertex_descriptor;
  typedef typename graph_traits<Graph>::edge_iterator     edge_iterator;

  double volume = 1.;
  for (std::size_t i = 0; i < Topology::point_difference_type::dimensions; ++i)
    volume *= extent[i];

  // assume positions are initialized randomly
  double k = pow(volume / num_vertices(g), 1. / (double)(Topology::point_difference_type::dimensions));

  detail::fr_apply_force<Topology, PositionMap, DisplacementMap,
                         RepulsiveForce, Graph>
    apply_force(topology, position, displacement, origin, extent, repulsive_force, k, g);

  do {
    // Calculate repulsive forces
    vertex_iterator v, v_end;
    for (tie(v, v_end) = vertices(g); v != v_end; ++v)
      displacement[*v] = typename Topology::point_difference_type();
    force_pairs(g, apply_force);

    // Calculate attractive forces
    edge_iterator e, e_end;
    for (tie(e, e_end) = edges(g); e != e_end; ++e) {
      vertex_descriptor v = source(*e, g);
      vertex_descriptor u = target(*e, g);

      // When the vertices land on top of each other, move the
      // first vertex away from the boundaries.
      ::boost::detail::maybe_jitter_point(topology, position[u], position[v], 
                                          origin, extent);

      typename Topology::point_difference_type delta = topology.difference(position[v], position[u]);
      double dist = topology.distance(position[u], position[v]);
      double fa = attractive_force(*e, k, dist, g);

      displacement[v] -= (fa / dist) * delta;
      displacement[u] += (fa / dist) * delta;
    }

    if (double temp = cool()) {
      // Update positions
      BGL_FORALL_VERTICES_T (v, g, Graph) {
        BOOST_USING_STD_MIN();
        BOOST_USING_STD_MAX();
        double disp_size = topology.norm(displacement[v]);
        position[v] = topology.adjust(position[v], displacement[v] * (min BOOST_PREVENT_MACRO_SUBSTITUTION (disp_size, temp) / disp_size));
        position[v] = topology.bound(position[v]);
#if 0
        // CEM HACK: Jitter if we're on the edges
        if(position[v].x == 1.0f) // origin.x + extent.x)
          position[v].x -= drand48() * .1 * extent.x;
        else if(position[v].x == -1.0f) // origin.x)
          position[v].x += drand48() * .1 * extent.x;
#endif
      }
    } else {
      break;
    }
  } while (true);
}

namespace detail {
  template<typename DisplacementMap>
  struct fr_force_directed_layout
  {
    template<typename Topology, typename Graph, typename PositionMap, 
             typename AttractiveForce, typename RepulsiveForce,
             typename ForcePairs, typename Cooling,
             typename Param, typename Tag, typename Rest>
    static void
    run(const Graph&    g,
        PositionMap     position,
        const Topology& topology,
        typename Topology::point_type const& origin,
        typename Topology::point_difference_type const& extent,
        AttractiveForce attractive_force,
        RepulsiveForce  repulsive_force,
        ForcePairs      force_pairs,
        Cooling         cool,
        DisplacementMap displacement,
        const bgl_named_params<Param, Tag, Rest>&)
    {
      fruchterman_reingold_force_directed_layout
        (g, position, topology, origin, extent, attractive_force, repulsive_force,
         force_pairs, cool, displacement);
    }
  };

  template<>
  struct fr_force_directed_layout<error_property_not_found>
  {
    template<typename Topology, typename Graph, typename PositionMap, 
             typename AttractiveForce, typename RepulsiveForce,
             typename ForcePairs, typename Cooling,
             typename Param, typename Tag, typename Rest>
    static void
    run(const Graph&    g,
        PositionMap     position,
        const Topology& topology,
        typename Topology::point_type const& origin,
        typename Topology::point_difference_type const& extent,
        AttractiveForce attractive_force,
        RepulsiveForce  repulsive_force,
        ForcePairs      force_pairs,
        Cooling         cool,
        error_property_not_found,
        const bgl_named_params<Param, Tag, Rest>& params)
    {
      typedef typename Topology::point_difference_type PointDiff;
      std::vector<PointDiff> displacements(num_vertices(g));
      fruchterman_reingold_force_directed_layout
        (g, position, topology, origin, extent, attractive_force, repulsive_force,
         force_pairs, cool,
         make_iterator_property_map
         (displacements.begin(),
          choose_const_pmap(get_param(params, vertex_index), g,
                            vertex_index),
          PointDiff()));
    }
  };

} // end namespace detail

template<typename Topology, typename Graph, typename PositionMap, typename Param,
         typename Tag, typename Rest>
void
fruchterman_reingold_force_directed_layout
  (const Graph&    g,
   PositionMap     position,
   const Topology& topology,
   typename Topology::point_type const& origin,
   typename Topology::point_difference_type const& extent,
   const bgl_named_params<Param, Tag, Rest>& params)
{
  typedef typename property_value<bgl_named_params<Param,Tag,Rest>,
                                  vertex_displacement_t>::type D;

  detail::fr_force_directed_layout<D>::run
    (g, position, topology, origin, extent,
     choose_param(get_param(params, attractive_force_t()),
                  square_distance_attractive_force()),
     choose_param(get_param(params, repulsive_force_t()),
                  square_distance_repulsive_force()),
     choose_param(get_param(params, force_pairs_t()),
                  make_grid_force_pairs(topology, origin, extent, position, g)),
     choose_param(get_param(params, cooling_t()),
                  linear_cooling<double>(100)),
     get_param(params, vertex_displacement_t()),
     params);
}

template<typename Topology, typename Graph, typename PositionMap>
void
fruchterman_reingold_force_directed_layout
  (const Graph&    g,
   PositionMap     position,
   const Topology& topology,
   typename Topology::point_type const& origin,
   typename Topology::point_difference_type const& extent)
{
  fruchterman_reingold_force_directed_layout
    (g, position, topology, origin, extent,
     attractive_force(square_distance_attractive_force()));
}

} // end namespace boost

#ifdef BOOST_GRAPH_USE_MPI
#  include <boost/graph/distributed/fruchterman_reingold.hpp>
#endif

#endif // BOOST_GRAPH_FRUCHTERMAN_REINGOLD_FORCE_DIRECTED_LAYOUT_HPP
