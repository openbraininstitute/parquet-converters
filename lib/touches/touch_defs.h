/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#ifndef LIB_TOUCHES_TOUCH_DEFS_H_
#define LIB_TOUCHES_TOUCH_DEFS_H_

#include <algorithm>

namespace neuron_parquet {
namespace touches {

enum TOUCH_LOCATION { NEURON_ID, SECTION_ID, SEGMENT_ID };

struct Touch {
    inline int getPreNeuronID() const {
        return pre_synapse_ids[NEURON_ID];
    }
    inline int getPostNeuronID() const {
        return post_synapse_ids[NEURON_ID];
    }

    int pre_synapse_ids[3];
    int post_synapse_ids[3];
    int branch;
    float distance_soma;
    float pre_offset, post_offset;
};

struct IndexedTouch : public Touch {
    inline IndexedTouch& operator=(const Touch& t)
    {
        std::copy(t.pre_synapse_ids, t.pre_synapse_ids + 3, pre_synapse_ids);
        std::copy(t.post_synapse_ids, t.post_synapse_ids + 3, post_synapse_ids);
        branch = t.branch;
        distance_soma = t.distance_soma;
        pre_offset = t.pre_offset;
        post_offset = t.post_offset;

        return *this;
    };

    long synapse_index;
};

}  // namespace touches
}  // namespace neuron_parquet

#endif  // LIB_TOUCHES_TOUCH_DEFS_H_
