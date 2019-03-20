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
#include <iostream>

namespace neuron_parquet {
namespace touches {

enum Location { NEURON_ID, SECTION_ID, SEGMENT_ID };
enum Version { V1, V2 };

namespace v1 {
    struct Touch {
        int pre_synapse_ids[3];
        int post_synapse_ids[3];
        int branch;
        float distance_soma;
        float pre_offset, post_offset;
    };
}

namespace v2 {
    struct Touch : public v1::Touch {
        float pre_position[3];
        float post_position[3];
        float spine_length = -1;
        unsigned char branch_type = 255;

        Touch() = default;
        Touch(const Touch&) = default;
        Touch(const v1::Touch& t)
            : v1::Touch(t)
        {};
    };
}

struct IndexedTouch : public v2::Touch {
    int getPreNeuronID() const {
        return pre_synapse_ids[NEURON_ID];
    }
    int getPostNeuronID() const {
        return post_synapse_ids[NEURON_ID];
    }

    IndexedTouch() = default;
    IndexedTouch(const IndexedTouch&) = default;

    IndexedTouch(const v1::Touch& t, long index)
        : v2::Touch(t)
        , synapse_index(index)
    {};

    IndexedTouch(const v2::Touch& t, long index)
        : v2::Touch(t)
        , synapse_index(index)
    {};

    long synapse_index;
};

}  // namespace touches
}  // namespace neuron_parquet

#endif  // LIB_TOUCHES_TOUCH_DEFS_H_
