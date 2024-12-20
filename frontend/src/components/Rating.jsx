import { Star } from 'lucide-react'

function Rating({ rating, max = 5 }) {
    return (
        <div className="flex items-center">
            {Array.from({ length: max }).map((_, i) => (
                <Star
                    key={i}
                    className={`w-5 h-5 ${i < rating ? 'fill-secondaryclr text-secondaryclr' : 'text-gray-300'
                        }`}
                />
            ))}
        </div>
    )
}

export default Rating;