import { MapPin, Star } from 'lucide-react'

import PropTypes from 'prop-types'


const HotelCard = ({ name, image, rating, reviews, address }) => {
    return (
        <div className="rounded-lg border bg-card text-card-foreground shadow-sm">
            <div className="relative aspect-[4/3] overflow-hidden rounded-t-lg">
            <img className="object-cover" src={image} alt="Hotel" />
            </div>
            <div className="p-4">
                <h3 className="font-semibold text-lg mb-2">{name}</h3>
                <div className="flex items-center gap-1 mb-2">
                    {[...Array(5)].map((_, i) => (
                        <Star
                            key={i}
                            className={`h-4 w-4 ${i < rating ? "fill-primary text-primary" : "fill-muted text-muted-foreground"}`}
                        />
                    ))}
                    <span className="text-sm text-muted-foreground ml-2">{reviews} reviews</span>
                </div>
                <div className="flex items-center gap-2 text-sm text-muted-foreground">
                    <MapPin className="h-4 w-4" />
                    <span>{address}</span>
                </div>
            </div>
        </div>
    )
}
HotelCard.propTypes = {
    name: PropTypes.string.isRequired,
    image: PropTypes.string.isRequired,
    rating: PropTypes.number.isRequired,
    reviews: PropTypes.number.isRequired,
    address: PropTypes.string.isRequired
}

export default HotelCard;
